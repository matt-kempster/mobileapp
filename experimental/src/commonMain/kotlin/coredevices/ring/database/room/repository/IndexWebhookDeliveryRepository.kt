package coredevices.ring.database.room.repository

import coredevices.ring.data.entity.room.IndexWebhookDeliveryEntity
import coredevices.ring.database.room.dao.IndexWebhookDeliveryDao
import coredevices.ring.service.button.RingGesture
import coredevices.util.queue.QueueTaskRepository
import coredevices.util.queue.TaskStatus
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.first
import kotlin.time.Instant

class IndexWebhookDeliveryRepository(
    private val dao: IndexWebhookDeliveryDao,
) : QueueTaskRepository<IndexWebhookDeliveryEntity> {
    override suspend fun insertTask(task: IndexWebhookDeliveryEntity): Long = dao.insertTask(task)

    override suspend fun getPendingTasks(): List<IndexWebhookDeliveryEntity> = dao.getPendingTasks()

    override suspend fun updateLastSuccessfulStage(taskId: Long, stage: String) = Unit

    override suspend fun updateStatus(taskId: Long, status: TaskStatus) = dao.updateTaskStatus(taskId, status)

    override suspend fun deleteTask(taskId: Long) = dao.deleteTask(taskId)

    override suspend fun deleteCompletedTasksAttemptedBefore(before: Instant) =
        dao.deleteCompletedTasksAttemptedBefore(before)

    override suspend fun getTaskById(taskId: Long): IndexWebhookDeliveryEntity? = dao.getTaskById(taskId)

    override suspend fun incrementAttempts(taskId: Long, currentTime: Instant) =
        dao.incrementAttempts(taskId, currentTime)

    fun undeliveredIds(gesture: RingGesture): Flow<List<Long>> = dao.undeliveredIds(gesture)

    /** Puts every undelivered delivery back to pending with no attempts used; returns their task ids. */
    suspend fun resetUndelivered(gesture: RingGesture): List<Long> {
        dao.resetUndelivered(gesture)
        return dao.undeliveredIds(gesture).first()
    }
}
