Master branch is protected.

For developing create branch. Branch name must be started with "feature-". Merge only by Gitlab.

When created merge request Jenkins job started. https://spb99-jenkins.ertelecom.ru/job/DataEngineering/job/DEVELOPMENT/job/kafka-extract-deserializers-merge-test/
also job starts on push to source branch.

Job will checkout branches from merge request, merge them locally and run "sbt test".

If job succeeded, merge request can be accepted.