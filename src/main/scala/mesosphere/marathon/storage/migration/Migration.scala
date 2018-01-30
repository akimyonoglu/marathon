package mesosphere.marathon
package storage.migration

import akka.actor.Scheduler
import java.net.URI

import akka.Done
import akka.stream.Materializer
import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon.Protos.StorageVersion
import mesosphere.marathon.core.async.ExecutionContexts.global
import mesosphere.marathon.core.storage.backup.PersistentStoreBackup
import mesosphere.marathon.core.storage.store.PersistenceStore
import mesosphere.marathon.storage.StorageConfig
import mesosphere.marathon.storage.repository._
import mesosphere.marathon.util.toRichFuture

import scala.async.Async.{ async, await }
import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }
import scala.util.control.NonFatal
import mesosphere.marathon.raml.RuntimeConfiguration
import mesosphere.marathon.storage.migration.Migration.MigrationAction

import scala.concurrent.ExecutionContext
import scala.util.{ Failure, Success }

/**
  * Base trait of a migration step.
  */
trait MigrationStep {

  /**
    * Apply migration step.
    *
    * @param ctx
    * @param mat
    * @return Future for the running migration.
    */
  def migrate()(implicit ctx: ExecutionContext, mat: Materializer): Future[Done]
}

/**
  * @param persistenceStore Optional "new" PersistenceStore for new migrations, the repositories
  *                         are assumed to be in the new format.
  */
@SuppressWarnings(Array("UnusedMethodParameter")) // materializer will definitely be used in the future.
class Migration(
    private[migration] val availableFeatures: Set[String],
    private[migration] val defaultNetworkName: Option[String],
    private[migration] val mesosBridgeName: String,
    private[migration] val persistenceStore: PersistenceStore[_, _, _],
    private[migration] val appRepository: AppRepository,
    private[migration] val podRepository: PodRepository,
    private[migration] val groupRepository: GroupRepository,
    private[migration] val deploymentRepository: DeploymentRepository,
    private[migration] val instanceRepo: InstanceRepository,
    private[migration] val taskFailureRepo: TaskFailureRepository,
    private[migration] val frameworkIdRepo: FrameworkIdRepository,
    private[migration] val serviceDefinitionRepo: ServiceDefinitionRepository,
    private[migration] val runtimeConfigurationRepository: RuntimeConfigurationRepository,
    private[migration] val backup: PersistentStoreBackup,
    private[migration] val config: StorageConfig,
    private[migration] val steps: List[MigrationAction] = Migration.steps
)(implicit mat: Materializer, scheduler: Scheduler) extends StrictLogging {

  import StorageVersions.OrderedStorageVersion
  import Migration.statusLoggingInterval

  private[migration] val minSupportedStorageVersion = StorageVersions(1, 4, 0, StorageVersion.StorageFormat.PERSISTENCE_STORE)

  val targetVersion = StorageVersions(steps)

  protected def notifyMigrationInProgress(from: StorageVersion, migrateVersion: StorageVersion) = {
    logger.info(
      s"Migration for storage: ${from.str} to current: ${targetVersion.str}: " +
        s"application of the change for version ${migrateVersion.str} is still in progress"
    )
  }

  def applyMigrationSteps(from: StorageVersion): Future[Seq[StorageVersion]] = {
    steps.filter(_._1 > from).sortBy(_._1).foldLeft(Future.successful(Seq.empty[StorageVersion])) {
      case (resultsFuture, (migrateVersion, change)) => resultsFuture.flatMap { res =>
        logger.info(
          s"Migration for storage: ${from.str} to target: ${targetVersion.str}: apply change for version: ${migrateVersion.str} "
        )

        val migrationInProgressNotification = scheduler.schedule(statusLoggingInterval, statusLoggingInterval) {
          notifyMigrationInProgress(from, migrateVersion)
        }

        val step = change.apply(this)
        step.migrate().recover {
          case e: MigrationCancelledException => throw e
          case NonFatal(e) =>
            throw new MigrationFailedException(s"while migrating storage to $migrateVersion", e)
        }.map { _ =>
          res :+ migrateVersion
        }.andThen {
          case _ =>
            migrationInProgressNotification.cancel()
        }
      }
    }
  }

  @SuppressWarnings(Array("all")) // async/await
  def migrateAsync(): Future[Seq[StorageVersion]] = async {

    val config = await(runtimeConfigurationRepository.get()).getOrElse(RuntimeConfiguration())
    // before backup/restore called, reset the runtime configuration
    await(runtimeConfigurationRepository.store(RuntimeConfiguration(None, None)))
    // step 1: backup current zk state
    await(config.backup.map(uri => backup.backup(new URI(uri))).getOrElse(Future.successful(Done)))
    // step 2: restore state from given backup
    await(config.restore.map(uri => backup.restore(new URI(uri))).getOrElse(Future.successful(Done)))

    // mark migration as started
    await(persistenceStore.startMigration())

    // run the migration, to ensure we can operate on the zk state
    await(migrateStorage(backupCreated = config.backup.isDefined || config.restore.isDefined).asTry) match {
      case Success(result) =>
        // mark migration as completed
        await(persistenceStore.endMigration())

        logger.info(s"Migration successfully applied for version ${targetVersion.str}")
        result
      case Failure(ex: MigrationCancelledException) =>
        logger.error(ex.getMessage)

        // mark migration as completed
        await(persistenceStore.endMigration())

        throw new MigrationFailedException("Migration cancelled", ex.getCause)
      case Failure(ex) =>
        throw ex
    }
  }

  def migrate(): Seq[StorageVersion] =
    Await.result(migrateAsync(), Duration.Inf)

  @SuppressWarnings(Array("all")) // async/await
  def migrateStorage(backupCreated: Boolean = false): Future[Seq[StorageVersion]] = {
    async {
      val currentVersion = await(getCurrentVersion)

      val migrations = currentVersion match {
        case Some(version) if version < minSupportedStorageVersion =>
          val msg = s"Migration from versions < ${minSupportedStorageVersion.str} are not supported. Your version: ${version.str}"
          throw new MigrationFailedException(msg)
        case Some(version) if version > targetVersion =>
          val msg = s"Migration from ${version.str} is not supported as it is newer than ${targetVersion.str}."
          throw new MigrationFailedException(msg)
        case Some(version) if version < targetVersion =>
          if (!backupCreated && config.backupLocation.isDefined) {
            logger.info("Backup current state")
            await(backup.backup(config.backupLocation.get))
            logger.info("Backup finished. Apply migration.")
          }
          val result = await(applyMigrationSteps(version))
          await(storeCurrentVersion())
          result
        case Some(version) if version == targetVersion =>
          logger.info("No migration necessary, already at the current version")
          Nil
        case _ =>
          logger.info("No migration necessary, no version stored")
          await(storeCurrentVersion())
          Nil
      }
      migrations
    }.recover {
      case ex: MigrationCancelledException => throw ex
      case ex: MigrationFailedException => throw ex
      case NonFatal(ex) =>
        throw new MigrationFailedException(s"Migration Failed: ${ex.getMessage}", ex)
    }
  }

  private def getCurrentVersion: Future[Option[StorageVersion]] =
    persistenceStore.storageVersion()

  private def storeCurrentVersion(): Future[Done] =
    persistenceStore.setStorageVersion(targetVersion)
}

object Migration {
  val StorageVersionName = "internal:storage:version"
  val maxConcurrency = 8
  val statusLoggingInterval = 10.seconds

  type MigrationFactory = Migration => MigrationStep
  type MigrationAction = (StorageVersion, MigrationFactory)

  /**
    * All the migration steps, that have to be applied.
    * They get applied after the master has been elected.
    */
  lazy val steps: List[MigrationAction] =
    List(
      StorageVersions(1, 4, 2, StorageVersion.StorageFormat.PERSISTENCE_STORE) -> { migration =>
        new MigrationTo142(migration.appRepository)
      },
      StorageVersions(1, 4, 6, StorageVersion.StorageFormat.PERSISTENCE_STORE) -> { (migration) =>
        new MigrationTo146(migration.appRepository, migration.podRepository)
      },
      StorageVersions(1, 5, 0, StorageVersion.StorageFormat.PERSISTENCE_STORE) -> { (migration) =>
        MigrationTo15(migration)
      },
      StorageVersions(1, 5, 2, StorageVersion.StorageFormat.PERSISTENCE_STORE) -> { (migration) =>
        new MigrationTo152(migration.instanceRepo)
      },
      // From here onwards we are not bound to the build version anymore.
      StorageVersions(200) -> { (migration) =>
        new MigrationTo200(migration.instanceRepo, migration.persistenceStore)
      }
    )
}

object StorageVersions {

  def apply(major: Int, minor: Int = 0, patch: Int = 0,
    format: StorageVersion.StorageFormat = StorageVersion.StorageFormat.PERSISTENCE_STORE): StorageVersion = {
    StorageVersion
      .newBuilder()
      .setMajor(major)
      .setMinor(minor)
      .setPatch(patch)
      .setFormat(format)
      .build()
  }

  /**
    * Get the migration target version from a list of migration steps.
    *
    * @param steps
    * @return The target version of the migration steps.
    */
  def apply(steps: List[MigrationAction]): StorageVersion = steps.map(action => action._1).max

  implicit class OrderedStorageVersion(val version: StorageVersion) extends AnyVal with Ordered[StorageVersion] {
    override def compare(that: StorageVersion): Int = {
      def by(left: Int, right: Int, fn: => Int): Int = if (left.compareTo(right) != 0) left.compareTo(right) else fn
      by(version.getFormat.getNumber, that.getFormat.getNumber,
        by(version.getMajor, that.getMajor,
          by(version.getMinor, that.getMinor,
            by(version.getPatch, that.getPatch, 0))))
    }

    def str: String = s"Version(${version.getMajor}, ${version.getMinor}, ${version.getPatch}, ${version.getFormat})"

    def nonEmpty: Boolean = !version.equals(empty)
  }

  def empty: StorageVersion = StorageVersions(0, 0, 0, StorageVersion.StorageFormat.LEGACY)
}
