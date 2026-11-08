package coredevices.ring.external.indexwebhook

import com.russhwolf.settings.MapSettings
import coredevices.ring.service.button.RingGesture
import coredevices.ring.data.entity.room.IndexWebhookDeliveryEntity
import coredevices.util.integrations.IntegrationTokenStorage
import coredevices.util.queue.TaskStatus
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.resetMain
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.test.setMain
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Instant
import kotlin.time.Duration.Companion.seconds

@OptIn(ExperimentalCoroutinesApi::class)
class IndexWebhookSettingsViewModelTest {

    @Test
    fun saveStoresOnlyTheSigningFlagInPreferencesAndSecretInSecureStorage() = runTest {
        val dispatcher = StandardTestDispatcher(testScheduler)
        Dispatchers.setMain(dispatcher)
        try {
            val fixture = Fixture(backgroundScope)
            fixture.viewModel.openDialog(RingGesture.Hold)
            advanceUntilIdle()

            fixture.viewModel.updateUrlInput("https://example.com/hook")
            fixture.viewModel.updateSignRequests(true)
            fixture.viewModel.updateSigningSecret("  exact secret value  ")
            fixture.viewModel.save()
            advanceUntilIdle()

            val config = fixture.preferences.configFor(RingGesture.Hold)
            assertTrue(config.signRequests)
            assertEquals("https://example.com/hook", config.url)
            assertEquals(
                "  exact secret value  ",
                fixture.secretStorage.get(RingGesture.Hold),
            )
            assertFalse(fixture.settings.keys.any { key ->
                fixture.settings.getStringOrNull(key)?.contains("exact secret value") == true
            })
        } finally {
            Dispatchers.resetMain()
        }
    }

    @Test
    fun copyAndTestEventUseTheOtherGesturesSigningSecret() = runTest {
        val dispatcher = StandardTestDispatcher(testScheduler)
        Dispatchers.setMain(dispatcher)
        try {
            val fixture = Fixture(backgroundScope)
            fixture.preferences.setConfig(
                RingGesture.ClickHold,
                IndexWebhookConfig(
                    url = "https://other.example/hook",
                    signRequests = true,
                    saved = true,
                ),
            )
            fixture.secretStorage.save(RingGesture.ClickHold, "other-secret")

            fixture.viewModel.openDialog(RingGesture.Hold)
            advanceUntilIdle()
            fixture.viewModel.copyFromOtherGesture()
            advanceUntilIdle()

            assertEquals("https://other.example/hook", fixture.viewModel.urlInput.value)
            assertTrue(fixture.viewModel.signRequestsInput.value)
            assertEquals("other-secret", fixture.viewModel.signingSecretInput.value)

            fixture.viewModel.sendTestEvent()
            advanceUntilIdle()

            assertEquals(
                TestRequest(
                    gesture = RingGesture.Hold,
                    url = "https://other.example/hook",
                    signRequests = true,
                    signingSecret = "other-secret",
                ),
                fixture.api.lastTestRequest,
            )
        } finally {
            Dispatchers.resetMain()
        }
    }

    @Test
    fun copyOnlyOffersGesturesOfTheSameKind() = runTest {
        val dispatcher = StandardTestDispatcher(testScheduler)
        Dispatchers.setMain(dispatcher)
        try {
            val fixture = Fixture(backgroundScope)
            fixture.preferences.setConfig(
                RingGesture.Hold,
                IndexWebhookConfig(url = "https://hold.example/hook", saved = true),
            )

            fixture.viewModel.openDialog(RingGesture.Click)
            advanceUntilIdle()
            assertNull(fixture.viewModel.copyableGesture.value)

            fixture.preferences.setConfig(
                RingGesture.TripleClick,
                IndexWebhookConfig(url = "https://click.example/hook", saved = true),
            )
            advanceUntilIdle()
            assertEquals(RingGesture.TripleClick, fixture.viewModel.copyableGesture.value)
        } finally {
            Dispatchers.resetMain()
        }
    }

    @Test
    fun removingWebhookAlsoDeletesItsSigningSecret() = runTest {
        val dispatcher = StandardTestDispatcher(testScheduler)
        Dispatchers.setMain(dispatcher)
        try {
            val fixture = Fixture(backgroundScope)
            fixture.preferences.setConfig(
                RingGesture.Hold,
                IndexWebhookConfig(
                    url = "https://example.com/hook",
                    signRequests = true,
                    saved = true,
                ),
            )
            fixture.secretStorage.save(RingGesture.Hold, "secret")

            fixture.viewModel.openDialog(RingGesture.Hold)
            advanceUntilIdle()
            fixture.viewModel.removeCurrentGesture()
            advanceUntilIdle()

            assertEquals(IndexWebhookConfig(), fixture.preferences.configFor(RingGesture.Hold))
            assertNull(fixture.secretStorage.get(RingGesture.Hold))
        } finally {
            Dispatchers.resetMain()
        }
    }

    @Test
    fun countsUndeliveredRecordingsAndSendsThemNow() = runTest {
        val dispatcher = StandardTestDispatcher(testScheduler)
        Dispatchers.setMain(dispatcher)
        try {
            val fixture = Fixture(backgroundScope)
            fun delivery(id: String, status: TaskStatus) = IndexWebhookDeliveryEntity(
                status = status,
                deliveryId = id,
                gesture = RingGesture.Hold,
                recordingId = 1,
                fileId = null,
                transcription = null,
            )
            fixture.deliveryDao.add(delivery("failed", TaskStatus.Failed))
            fixture.deliveryDao.add(delivery("waiting", TaskStatus.Pending))
            fixture.deliveryDao.add(delivery("delivered", TaskStatus.Success))

            fixture.viewModel.openDialog(RingGesture.Hold)
            advanceUntilIdle()
            assertEquals(2, fixture.viewModel.undelivered.value)

            fixture.viewModel.sendUndelivered()
            advanceUntilIdle()
            // The queue sends on Dispatchers.IO; wait for it, and let it finish before Main is reset.
            withContext(Dispatchers.Default) {
                withTimeout(5.seconds) {
                    while (
                        fixture.deliveryDao.tasks.values.any { it.status != TaskStatus.Success } ||
                        fixture.deliveryQueue.activeTaskIds.value.isNotEmpty()
                    ) delay(10)
                }
            }
            advanceUntilIdle()
            assertEquals(0, fixture.viewModel.undelivered.value)
        } finally {
            Dispatchers.resetMain()
        }
    }

    private class Fixture(scope: CoroutineScope) {
        val settings = MapSettings()
        val preferences = IndexWebhookPreferences(settings)
        private val tokenStorage = MemoryTokenStorage()
        val secretStorage = IndexWebhookSigningSecretStorage(tokenStorage)
        val api = FakeWebhookApi()
        val runRepository = IndexWebhookRunRepository(settings)
        val deliveryDao = FakeWebhookDeliveryDao()
        val deliveryQueue = testDeliveryQueue(scope, dao = deliveryDao)
        val viewModel = IndexWebhookSettingsViewModel(
            preferences,
            api,
            runRepository,
            secretStorage,
            deliveryQueue,
        )
    }

    private data class TestRequest(
        val gesture: RingGesture,
        val url: String,
        val signRequests: Boolean,
        val signingSecret: String?,
    )

    private class FakeWebhookApi : IndexWebhookApi {
        var lastTestRequest: TestRequest? = null

        override suspend fun upload(
            samples: ShortArray?,
            sampleRate: Int,
            recordingId: String,
            transcription: String?,
            recordedAt: Instant,
            gesture: RingGesture,
        ): IndexWebhookRunResult? = null

        override fun sendButtonEventIfEnabled(gesture: RingGesture, pressedAt: Instant) = Unit

        override suspend fun sendTestEvent(
            gesture: RingGesture,
            url: String,
            headers: Map<String, String>,
            signRequests: Boolean,
            signingSecret: String?,
            clickPayload: String,
        ): IndexWebhookRunResult {
            lastTestRequest = TestRequest(gesture, url, signRequests, signingSecret)
            return IndexWebhookRunResult(true, "200 OK", "test event", 0, 0)
        }
    }

    private class MemoryTokenStorage : IntegrationTokenStorage {
        private val values = mutableMapOf<String, String>()

        override suspend fun saveToken(key: String, token: String) {
            values[key] = token
        }

        override suspend fun getToken(key: String): String? = values[key]

        override suspend fun deleteToken(key: String) {
            values.remove(key)
        }
    }
}
