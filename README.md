# cqrs

CQRS Mixins

Make you life easy when doing CQRS back-ends :)

```

```



## Snapshots

snapshotName := aggregate.snapshot():
 - pause event pumps -> pause aggregate changes :)
 - create a snapshot event -> new events will pile up after this point! = Write is enabled.
 - ** no changes are happening on aggregates ** but reads continue happily.
 - backup aggregates -> aggregate.backup(snapshotName)  (SQLLite -> basicaly copy files :) )
 - restart the event pump :)
 - done.
 - Error scenarios:
 - if backup fails the event is marked as failed and is ignored when trying to restore events.
 - Rationale:
 ---> Since you created an event about the start of the snapshot at the same moment you paused the pump. this event should point to the backup file. so it can be used when restoring the snapshot.


 The restore an snapshot is also very simple
 aggregate.restore(snapshotName)
 - Condition: event pump is paused.
 - find backup files using snapshotName and locate snapshot event in the event store.
 - ** at this stage the event store might be receiving new events -> write is enabled **
 - backup is restored.
 - read is enabled :)
 - events start processing from the snapshot moment
 - ** system takes a while to catch up **
 - system is eventually consistent :)


## Event Store 