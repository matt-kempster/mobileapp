package coredevices.ring.service.recordings.button

import coredevices.mcp.data.SemanticResult
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class WebhookOnlyNotificationTest {

    private val delivered = SemanticResult.ListItemCreation("buy milk", destination = WEBHOOK_ONLY_DESTINATION)

    @Test
    fun successfulWebhookOnlyDeliveryGetsNoActionNotification() {
        assertTrue(listOf(delivered).isOnlyWebhookDelivery())
    }

    @Test
    fun realNotesStillNotify() {
        assertFalse(listOf(SemanticResult.ListItemCreation("buy milk")).isOnlyWebhookDelivery())
        assertFalse(listOf(SemanticResult.ListItemCreation("buy milk", destination = "Notion")).isOnlyWebhookDelivery())
    }

    @Test
    fun failedDeliveryStillNotifies() {
        assertFalse(listOf(SemanticResult.GenericFailure(userErrorMessage = "Couldn't reach the webhook")).isOnlyWebhookDelivery())
    }

    @Test
    fun anyOtherActionStillNotifies() {
        assertFalse(listOf(delivered, SemanticResult.GenericSuccess).isOnlyWebhookDelivery())
        assertFalse(emptyList<SemanticResult>().isOnlyWebhookDelivery())
    }
}
