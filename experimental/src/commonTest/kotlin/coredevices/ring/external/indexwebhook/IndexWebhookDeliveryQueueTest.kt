package coredevices.ring.external.indexwebhook

import com.russhwolf.settings.MapSettings
import coredevices.indexai.data.entity.LocalRecording
import coredevices.indexai.database.dao.LocalRecordingDao
import coredevices.ring.data.entity.room.CachedRecordingMetadata
import coredevices.ring.data.entity.room.IndexWebhookDeliveryEntity
import coredevices.ring.database.room.dao.IndexWebhookDeliveryDao
import coredevices.ring.database.room.repository.IndexWebhookDeliveryRepository
import coredevices.ring.service.RecordingBackgroundScope
import coredevices.ring.service.button.RingGesture
import coredevices.ring.storage.RecordingStorage
import coredevices.ring.storage.StagedRecordingUpload
import coredevices.util.queue.RecoverableTaskException
import coredevices.util.queue.TaskStatus
import io.ktor.http.HttpStatusCode
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.io.Buffer
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.AfterTest
import kotlin.time.Instant
import kotlin.time.Duration.Companion.seconds

class IndexWebhookDeliveryQueueTest {

    private val created = Instant.fromEpochMilliseconds(1_700_000_000_000)

    private fun delivery(
        fileId: String? = null,
        transcription: String? = "hello",
        deliveryId: String = fileId ?: "text-7",
        status: TaskStatus = TaskStatus.Pending,
        attempts: Int = 0,
        gesture: RingGesture = RingGesture.Hold,
    ) = IndexWebhookDeliveryEntity(
        id = 1,
        created = created,
        attempts = attempts,
        status = status,
        deliveryId = deliveryId,
        gesture = gesture,
        recordingId = 7,
        fileId = fileId,
        transcription = transcription,
    )

    private val runRepository = IndexWebhookRunRepository(MapSettings())

    private fun TestScope.queue(api: FakeWebhookApi) = testDeliveryQueue(backgroundScope, api, runRepository)

    /** The scheduler sends on Dispatchers.IO, so tests that run it end to end wait in real time. */
    private val realScope = CoroutineScope(Dispatchers.Default + SupervisorJob())

    @AfterTest
    fun tearDown() = realScope.cancel()

    private suspend fun awaitUntil(condition: suspend () -> Boolean) = withContext(Dispatchers.Default) {
        withTimeout(5.seconds) { while (!condition()) delay(10) }
    }

    @Test
    fun sendsTheQueuedPayload() = runTest {
        val api = FakeWebhookApi(result(ok = true))
        queue(api).processTask(delivery())

        val call = api.calls.single()
        assertEquals("text-7", call.recordingId)
        assertEquals("hello", call.transcription)
        assertNull(call.samples)
        assertEquals(RingGesture.Hold, call.gesture)
        // The recording is gone, so its capture time falls back to when the delivery was queued.
        assertEquals(created, call.recordedAt)
    }

    @Test
    fun readsAudioFromTheRecordingWhenSending() = runTest {
        val api = FakeWebhookApi(result(ok = true))
        queue(api).processTask(delivery(fileId = "rec-1", transcription = null))

        assertEquals(listOf<Short>(1, -2), api.calls.single().samples?.toList())
    }

    @Test
    fun retryableFailureIsRecoverable() = runTest {
        val api = FakeWebhookApi(result(ok = false, retryable = true))
        assertFailsWith<RecoverableTaskException> { queue(api).processTask(delivery()) }
    }

    @Test
    fun permanentFailureIsNotRecoverable() = runTest {
        val api = FakeWebhookApi(result(ok = false, retryable = false))
        assertFailsWith<IllegalStateException> { queue(api).processTask(delivery()) }
    }

    @Test
    fun sendNowGivesEveryUndeliveredRecordingAFreshRoundAndSendsIt() = runTest {
        val dao = FakeWebhookDeliveryDao()
        val gaveUp = dao.add(
            delivery(deliveryId = "gave-up", status = TaskStatus.Failed, attempts = IndexWebhookDeliveryQueue.MAX_ATTEMPTS)
        )
        val interrupted = dao.add(delivery(deliveryId = "interrupted", status = TaskStatus.Failed, attempts = 1))
        val waiting = dao.add(delivery(deliveryId = "waiting", status = TaskStatus.Pending, attempts = 3))
        val delivered = dao.add(delivery(deliveryId = "delivered", status = TaskStatus.Success, attempts = 1))
        val otherGesture = dao.add(delivery(deliveryId = "other", status = TaskStatus.Failed, gesture = RingGesture.ClickHold))
        val api = FakeWebhookApi(result(ok = true))
        val queue = testDeliveryQueue(realScope, api, runRepository, dao)

        assertEquals(listOf(gaveUp, interrupted, waiting), queue.undeliveredIds(RingGesture.Hold).first())
        queue.sendNow(RingGesture.Hold)
        awaitUntil { queue.undeliveredIds(RingGesture.Hold).first().isEmpty() }

        assertEquals(listOf("gave-up", "interrupted", "waiting"), api.calls.map { it.recordingId })
        listOf(gaveUp, interrupted, waiting).forEach { assertEquals(1, dao.tasks.getValue(it).attempts) }
        assertEquals(TaskStatus.Success, dao.tasks.getValue(delivered).status)
        assertEquals(TaskStatus.Failed, dao.tasks.getValue(otherGesture).status)
    }

    @Test
    fun disabledWebhookIsDone() = runTest {
        val api = FakeWebhookApi(result = null)
        queue(api).processTask(delivery())
        assertEquals(1, api.calls.size)
    }

    @Test
    fun onlyTransientStatusesAreRetryable() {
        listOf(408, 429, 500, 502, 503, 504).forEach {
            assertTrue(HttpStatusCode.fromValue(it).isRetryableWebhookStatus(), "$it")
        }
        listOf(400, 401, 403, 404, 409, 413, 422).forEach {
            assertFalse(HttpStatusCode.fromValue(it).isRetryableWebhookStatus(), "$it")
        }
    }

    private fun result(ok: Boolean, retryable: Boolean = false) = IndexWebhookRunResult(
        ok = ok,
        status = if (ok) "200 OK" else "503 ERROR",
        detail = "",
        byteSize = 0,
        durationMs = 0,
        retryable = retryable,
    )
}

internal fun testDeliveryQueue(
    scope: CoroutineScope,
    api: IndexWebhookApi = FakeWebhookApi(result = null),
    runRepository: IndexWebhookRunRepository = IndexWebhookRunRepository(MapSettings()),
    dao: IndexWebhookDeliveryDao = FakeWebhookDeliveryDao(),
) = IndexWebhookDeliveryQueue(
    repository = IndexWebhookDeliveryRepository(dao),
    webhookApi = api,
    runRepository = runRepository,
    recordingStorage = FakeRecordingStorage,
    localRecordingDao = FakeLocalRecordingDao,
    scope = RecordingBackgroundScope(scope),
    settings = MapSettings(),
)

private class FakeWebhookApi(private val result: IndexWebhookRunResult?) : IndexWebhookApi {
    data class Call(
        val samples: ShortArray?,
        val recordingId: String,
        val transcription: String?,
        val recordedAt: Instant,
        val gesture: RingGesture,
    )

    val calls = mutableListOf<Call>()

    override suspend fun upload(
        samples: ShortArray?,
        sampleRate: Int,
        recordingId: String,
        transcription: String?,
        recordedAt: Instant,
        gesture: RingGesture,
    ): IndexWebhookRunResult? {
        calls += Call(samples, recordingId, transcription, recordedAt, gesture)
        return result
    }

    override fun sendButtonEventIfEnabled(gesture: RingGesture, pressedAt: Instant) = Unit

    override suspend fun sendTestEvent(
        gesture: RingGesture,
        url: String,
        headers: Map<String, String>,
        signRequests: Boolean,
        signingSecret: String?,
        clickPayload: String,
    ) = throw NotImplementedError("unused")
}

/** Keeps deliveries in memory, so the queue can run against it end to end. */
internal class FakeWebhookDeliveryDao : IndexWebhookDeliveryDao {
    private val state = MutableStateFlow<Map<Long, IndexWebhookDeliveryEntity>>(emptyMap())
    val tasks: Map<Long, IndexWebhookDeliveryEntity> get() = state.value
    private var nextId = 1L

    fun add(task: IndexWebhookDeliveryEntity): Long {
        val id = nextId++
        state.update { it + (id to task.copy(id = id)) }
        return id
    }

    private fun change(taskId: Long, transform: (IndexWebhookDeliveryEntity) -> IndexWebhookDeliveryEntity) =
        state.update { tasks -> tasks[taskId]?.let { tasks + (taskId to transform(it)) } ?: tasks }

    override suspend fun insertTask(task: IndexWebhookDeliveryEntity): Long =
        if (tasks.values.any { it.deliveryId == task.deliveryId }) -1 else add(task)

    override suspend fun getPendingTasks() =
        tasks.values.filter { it.status == TaskStatus.Pending }.sortedBy { it.created }

    override suspend fun updateTaskStatus(taskId: Long, status: TaskStatus) = change(taskId) { it.copy(status = status) }

    override suspend fun deleteTask(taskId: Long) = state.update { it - taskId }

    override suspend fun deleteCompletedTasksAttemptedBefore(before: Instant) = TODO()

    override suspend fun getTaskById(taskId: Long) = tasks[taskId]

    override suspend fun incrementAttempts(taskId: Long, currentTime: Instant) =
        change(taskId) { it.copy(attempts = it.attempts + 1, lastAttempt = currentTime) }

    override fun undeliveredIds(gesture: RingGesture) = state.map { tasks ->
        tasks.values.filter { it.gesture == gesture && it.status != TaskStatus.Success }.map { it.id }
    }

    override suspend fun resetUndelivered(gesture: RingGesture) {
        state.update { tasks ->
            tasks.mapValues { (_, task) ->
                if (task.gesture == gesture && task.status != TaskStatus.Success) {
                    task.copy(status = TaskStatus.Pending, attempts = 0)
                } else {
                    task
                }
            }
        }
    }
}

private object FakeLocalRecordingDao : LocalRecordingDao {
    override suspend fun getRecording(id: Long): LocalRecording? = null
    override suspend fun insertRecording(recording: LocalRecording): Long = TODO()
    override suspend fun updateRecording(recording: LocalRecording) = TODO()
    override suspend fun updateRecordingFirestoreId(id: Long, firestoreId: String) = TODO()
    override suspend fun deleteRecording(recording: LocalRecording) = TODO()
    override suspend fun getByFirestoreId(firestoreId: String): LocalRecording? = TODO()
    override fun getRecordingFlow(id: Long) = TODO()
    override fun getAllRecordings() = TODO()
    override fun getAllRecordingsAfter(timestamp: Instant) = TODO()
    override fun getRecordingsCount() = TODO()
    override suspend fun getMostRecentTimestamp(): LocalRecording? = TODO()
    override suspend fun getRecentRecordings(limit: Int): List<LocalRecording> = TODO()
    override suspend fun getAllFirestoreIds(): List<String> = TODO()
    override suspend fun deleteAll() = TODO()
    override suspend fun setUpdated(id: Long, updated: Instant) = TODO()
    override suspend fun setLastPushedUpdated(id: Long, updated: Long) = TODO()
}

private object FakeRecordingStorage : RecordingStorage {
    override suspend fun openRecordingSource(
        idNoSuffix: String,
        useOriginalAudio: Boolean,
    ) = Buffer().apply { write(byteArrayOf(1, 0, -2, -1)) } to RecordingStorage.RecordingSourceInfo(
        id = idNoSuffix,
        cachedMetadata = CachedRecordingMetadata(id = idNoSuffix, sampleRate = 16000, mimeType = "audio/raw"),
        size = 4,
    )

    override fun getCacheDirectory() = TODO()
    override suspend fun exportRecording(id: String, useOriginalAudio: Boolean) = TODO()
    override suspend fun openRecordingSink(id: String, sampleRate: Int, mimeType: String) = TODO()
    override suspend fun openOriginalRecordingSink(id: String, sampleRate: Int, mimeType: String) = TODO()
    override suspend fun openCachedRecordingSource(idNoSuffix: String, useOriginalAudio: Boolean) = TODO()
    override suspend fun persistRecording(id: String) = TODO()
    override suspend fun uploadRecordingPcm(id: String, sampleRate: Int, pcmBytes: ByteArray, encryptionKey: String?) = TODO()
    override fun deleteRecording(id: String) = TODO()
    override fun deleteRecordingFromCache(id: String) = TODO()
    override fun recordingExists(id: String) = TODO()
    override suspend fun deleteAllCachedMetadata() = TODO()
    override fun clearCacheDirectory() = TODO()
    override suspend fun deleteFromFirebaseStorage(id: String) = TODO()
    override suspend fun uploadStagedRecording(upload: StagedRecordingUpload) = TODO()
    override fun deleteStagedRecording(id: String) = TODO()
}
