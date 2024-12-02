// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// suite("test_backup_restore_priv") {

//     String suiteName = "test_backup_restore_priv"
//     String repoName = "${suiteName}_repo"
//     String dbName = "${suiteName}_db"
//     String tableName = "${suiteName}_table"
//     String snapshotName = "${suiteName}_snapshot"
//     String snapshotName_1 = "${suiteName}_snapshot1"
//     String user = "${suiteName}_user"

//     def syncer = getSyncer()
//     syncer.createS3Repository(repoName)

//     sql "DROP DATABASE IF EXISTS ${dbName}"
//     sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
//     sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"


//     sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
//     sql "CREATE USER '${user}' IDENTIFIED BY '12345' PASSWORD_EXPIRE INTERVAL 10 DAY FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 1 DAY;"
//     sql """
//            CREATE TABLE if NOT EXISTS ${dbName}.${tableName} 
//            (
//                `test` INT,
//                `id` INT
//            )
//            ENGINE=OLAP
//            UNIQUE KEY(`test`, `id`)
//            DISTRIBUTED BY HASH(id) BUCKETS 1 
//            PROPERTIES ( 
//                "replication_allocation" = "tag.location.default: 1"  
//         )
//         """

//     logger.info("=== Test 1: Common backup and restore ===")
//     def insert_num = 5
//     for (int i = 0; i < insert_num; ++i) {
//         sql """
//                INSERT INTO ${dbName}.${tableName} VALUES (${i}, ${i})
//             """ 
//     }
//     sql " sync "
//     def res = sql "SELECT * FROM ${dbName}.${tableName}"
//     assertEquals(res.size(), insert_num)
//     sql """ 
//             BACKUP SNAPSHOT ${dbName}.${snapshotName} 
//             TO ${repoName} 
//             ON (${tableName})
//             PROPERTIES ("type" = "full")
//         """
    
//     syncer.waitSnapshotFinish(dbName)

//     def snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
//     logger.info("snapshot: ${snapshot}")

//     assertTrue(snapshot != null)

//     sql "DROP TABLE ${dbName}.${tableName}"
//     sql "DROP USER ${user}"

//     sql """
//         RESTORE SNAPSHOT ${dbName}.${snapshotName}
//         FROM `${repoName}`
//         ON ( `${tableName}`)
//         PROPERTIES
//         (
//             "backup_timestamp" = "${snapshot}",
//             "reserve_replica" = "true"
//         )
//     """

//     syncer.waitAllRestoreFinish(dbName)

//     res = sql "SELECT * FROM ${dbName}.${tableName}"
//     assertEquals(res.size(), insert_num)

//     // res = sql "select * from  information_schema.user_privileges where grantee like '%${user}%'"
//     // assertTrue(res.size() > 0)

//     def found = 0;
//     def records = sql_return_maparray "SHOW all grants"
//     for (def res2 : records) {
//         if (res2.UserIdentity.contains(user)) {
//             found = 1
//             break
//         }
//     }
//     assertTrue(found == 1)

//     //cleanup
//     sql "DROP USER ${user}"
//     sql "DROP TABLE ${dbName}.${tableName} FORCE"
//     sql "DROP DATABASE ${dbName} FORCE"
//     sql "DROP REPOSITORY `${repoName}`"
// }

// suite("test_backup_restore_priv1", "backup_restore") {
//     String suiteName = "test_backup_restore_priv1"
//     String repoName = "${suiteName}_repo"
//     String dbName = "${suiteName}_db"
//     String tableName = "${suiteName}_table"
//     String snapshotName = "${suiteName}_snapshot"

//     def syncer = getSyncer()
//     syncer.createS3Repository(repoName)

//     sql "CREATE DATABASE IF NOT EXISTS ${dbName}"

//     sql "drop user if exists user1;"
//     sql "drop user if exists user2;"
//     sql "drop user if exists user3;"

//     sql "drop role if exists role_select;"
//     sql "drop role if exists role_load;"


//     sql "CREATE USER 'user1' IDENTIFIED BY '12345' PASSWORD_EXPIRE INTERVAL 10 DAY FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 1 DAY;"
//     sql "CREATE USER 'user2' IDENTIFIED BY '12345' PASSWORD_EXPIRE INTERVAL 10 DAY FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 1 DAY;"
//     sql "CREATE USER 'user3' IDENTIFIED BY '12345' PASSWORD_EXPIRE INTERVAL 10 DAY FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 1 DAY;"

//     sql "create role role_select;"
//     sql "GRANT Select_priv ON *.* TO ROLE 'role_select';"

//     sql "create role role_load;"
//     sql "GRANT Load_priv ON *.* TO ROLE 'role_load';"

//     sql "grant 'role_select', 'role_load' to 'user1'@'%';"
//     sql "grant 'role_select' to 'user2'@'%';"
//     sql "grant 'role_load' to 'user3'@'%';"


//     qt_sql_before_restore1 "show all grants;"
//     qt_sql_before_restore2 "show roles;"
//     //except 'password_policy.password_creation_time'
//     qt_sql_before_restore3 "select Host,User,Node_priv,Admin_priv,Grant_priv,Select_priv ,Load_priv,Alter_priv ,Create_priv,Drop_priv,Usage_priv,Show_view_priv,Cluster_usage_priv, Stage_usage_priv,ssl_type,ssl_cipher,x509_issuer,x509_subject,max_questions,max_updates,max_connections,max_user_connections,plugin,authentication_string,'password_policy.expiration_seconds','password_policy.history_num', 'password_policy.history_passwords','password_policy.num_failed_login','password_policy.password_lock_seconds', 'password_policy.failed_login_counter','password_policy.lock_time' from mysql.user;"



//     sql """
//         BACKUP SNAPSHOT ${dbName}.${snapshotName}
//         TO `${repoName}`
//     """

//     syncer.waitSnapshotFinish(dbName)

//     sql "drop user if exists user1;"
//     sql "drop user if exists user2;"
//     sql "drop user if exists user3;"

//     sql "drop role if exists role_select;"
//     sql "drop role if exists role_load;"

//     qt_sql_1 "show all grants;"
//     qt_sql_2 "show roles;"
//     //except 'password_policy.password_creation_time'
//     qt_sql_3 "select Host,User,Node_priv,Admin_priv,Grant_priv,Select_priv ,Load_priv,Alter_priv ,Create_priv,Drop_priv,Usage_priv,Show_view_priv,Cluster_usage_priv, Stage_usage_priv,ssl_type,ssl_cipher,x509_issuer,x509_subject,max_questions,max_updates,max_connections,max_user_connections,plugin,authentication_string,'password_policy.expiration_seconds','password_policy.history_num', 'password_policy.history_passwords','password_policy.num_failed_login','password_policy.password_lock_seconds', 'password_policy.failed_login_counter','password_policy.lock_time' from mysql.user;"


//     def snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
//     assertTrue(snapshot != null)

//     sql """
//         RESTORE SNAPSHOT ${dbName}.${snapshotName}
//         FROM `${repoName}`
//         PROPERTIES
//         (
//             "backup_timestamp" = "${snapshot}",
//             "reserve_replica" = "true"
//         )
//     """

//     syncer.waitAllRestoreFinish(dbName)
 
//     qt_sql_after_restore1 "show all grants;"
//     qt_sql_after_restore2 "show roles;"
//     //except 'password_policy.password_creation_time'
//     qt_sql_after_restore3 "select Host,User,Node_priv,Admin_priv,Grant_priv,Select_priv ,Load_priv,Alter_priv ,Create_priv,Drop_priv,Usage_priv,Show_view_priv,Cluster_usage_priv, Stage_usage_priv,ssl_type,ssl_cipher,x509_issuer,x509_subject,max_questions,max_updates,max_connections,max_user_connections,plugin,authentication_string,'password_policy.expiration_seconds','password_policy.history_num', 'password_policy.history_passwords','password_policy.num_failed_login','password_policy.password_lock_seconds', 'password_policy.failed_login_counter','password_policy.lock_time' from mysql.user;"

//     sql "drop user if exists user1;"
//     sql "drop user if exists user2;"
//     sql "drop user if exists user3;"

//     sql "drop role if exists role_select;"
//     sql "drop role if exists role_load;"

//     sql "DROP DATABASE ${dbName} FORCE"
//     sql "DROP REPOSITORY `${repoName}`"
// }

// suite("test_backup_restore_priv2", "backup_restore") {
//     String suiteName = "test_backup_restore_priv2"
//     String repoName = "${suiteName}_repo"
//     String dbName = "${suiteName}_db"
//     String tableName = "${suiteName}_table"
//     String snapshotName = "${suiteName}_snapshot"

//     def syncer = getSyncer()
//     syncer.createS3Repository(repoName)

//     sql "CREATE DATABASE IF NOT EXISTS ${dbName}"

//     sql "drop user if exists user1;"
//     sql "drop user if exists user2;"
//     sql "drop user if exists user3;"

//     sql "drop role if exists role_select;"
//     sql "drop role if exists role_load;"
//     sql "drop row policy if exists test_row_policy_1 on test.t1;"


//     sql "CREATE USER 'user1' IDENTIFIED BY '12345' PASSWORD_EXPIRE INTERVAL 10 DAY FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 1 DAY;"
//     sql "CREATE USER 'user2' IDENTIFIED BY '12345' PASSWORD_EXPIRE INTERVAL 10 DAY FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 1 DAY;"
//     sql "CREATE USER 'user3' IDENTIFIED BY '12345' PASSWORD_EXPIRE INTERVAL 10 DAY FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 1 DAY;"

//     sql "create role role_select;"
//     sql "GRANT Select_priv ON *.* TO ROLE 'role_select';"

//     sql "create role role_load;"
//     sql "GRANT Load_priv ON *.* TO ROLE 'role_load';"

//     sql "grant 'role_select', 'role_load' to 'user1'@'%';"
//     sql "grant 'role_select' to 'user2'@'%';"
//     sql "grant 'role_load' to 'user3'@'%';"

//     sql "CREATE ROW POLICY test_row_policy_1 ON test.t1 AS RESTRICTIVE TO user1 USING (c1 = 1);"


//     qt_sql_before_restore1 "show all grants;"
//     qt_sql_before_restore2 "show roles;"
//     //except 'password_policy.password_creation_time'
//     qt_sql_before_restore3 "select Host,User,Node_priv,Admin_priv,Grant_priv,Select_priv ,Load_priv,Alter_priv ,Create_priv,Drop_priv,Usage_priv,Show_view_priv,Cluster_usage_priv, Stage_usage_priv,ssl_type,ssl_cipher,x509_issuer,x509_subject,max_questions,max_updates,max_connections,max_user_connections,plugin,authentication_string,'password_policy.expiration_seconds','password_policy.history_num', 'password_policy.history_passwords','password_policy.num_failed_login','password_policy.password_lock_seconds', 'password_policy.failed_login_counter','password_policy.lock_time' from mysql.user;"
//     qt_sql_before_restore4 "show row policy;"



//     sql """
//         BACKUP SNAPSHOT ${dbName}.${snapshotName}
//         TO `${repoName}`
//     """

//     syncer.waitSnapshotFinish(dbName)

//     sql "drop user if exists user1;"
//     sql "drop user if exists user2;"
//     sql "drop user if exists user3;"

//     sql "drop role if exists role_select;"
//     sql "drop role if exists role_load;"
//     sql "drop row policy if exists test_row_policy_1 on test.t1;"

//     qt_sql_1 "show all grants;"
//     qt_sql_2 "show roles;"
//     //except 'password_policy.password_creation_time'
//     qt_sql_3 "select Host,User,Node_priv,Admin_priv,Grant_priv,Select_priv ,Load_priv,Alter_priv ,Create_priv,Drop_priv,Usage_priv,Show_view_priv,Cluster_usage_priv, Stage_usage_priv,ssl_type,ssl_cipher,x509_issuer,x509_subject,max_questions,max_updates,max_connections,max_user_connections,plugin,authentication_string,'password_policy.expiration_seconds','password_policy.history_num', 'password_policy.history_passwords','password_policy.num_failed_login','password_policy.password_lock_seconds', 'password_policy.failed_login_counter','password_policy.lock_time' from mysql.user;"
//     qt_sql_4 "show row policy;"


//     def snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
//     assertTrue(snapshot != null)

//     sql """
//         RESTORE SNAPSHOT ${dbName}.${snapshotName}
//         FROM `${repoName}`
//         PROPERTIES
//         (
//             "backup_timestamp" = "${snapshot}",
//             "reserve_replica" = "true"
//         )
//     """

//     syncer.waitAllRestoreFinish(dbName)
 
//     qt_sql_after_restore1 "show all grants;"
//     qt_sql_after_restore2 "show roles;"
//     //except 'password_policy.password_creation_time'
//     qt_sql_after_restore3 "select Host,User,Node_priv,Admin_priv,Grant_priv,Select_priv ,Load_priv,Alter_priv ,Create_priv,Drop_priv,Usage_priv,Show_view_priv,Cluster_usage_priv, Stage_usage_priv,ssl_type,ssl_cipher,x509_issuer,x509_subject,max_questions,max_updates,max_connections,max_user_connections,plugin,authentication_string,'password_policy.expiration_seconds','password_policy.history_num', 'password_policy.history_passwords','password_policy.num_failed_login','password_policy.password_lock_seconds', 'password_policy.failed_login_counter','password_policy.lock_time' from mysql.user;"
//     qt_sql_after_restore4 "show row policy;"

//     sql "drop user if exists user1;"
//     sql "drop user if exists user2;"
//     sql "drop user if exists user3;"

//     sql "drop role if exists role_select;"
//     sql "drop role if exists role_load;"
//     sql "drop row policy if exists test_row_policy_1 on test.t1;"

//     sql "DROP DATABASE ${dbName} FORCE"
//     sql "DROP REPOSITORY `${repoName}`"
// }

// suite("test_backup_restore_priv3", "backup_restore") {
//     String suiteName = "test_backup_restore_priv3"
//     String repoName = "${suiteName}_repo"
//     String dbName = "${suiteName}_db"
//     String tableName = "${suiteName}_table"
//     String snapshotName = "${suiteName}_snapshot"
//     def tokens = context.config.jdbcUrl.split('/')
//     def url=tokens[0] + "//" + tokens[2] + "/" + dbName + "?"

//     def syncer = getSyncer()
//     syncer.createS3Repository(repoName)

//     sql "DROP DATABASE IF EXISTS ${dbName}"
//     sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
//     sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
//     sql """
//            CREATE TABLE if NOT EXISTS ${dbName}.${tableName} 
//            (
//                `test` INT,
//                `id` INT
//            )
//            ENGINE=OLAP
//            UNIQUE KEY(`test`, `id`)
//            DISTRIBUTED BY HASH(id) BUCKETS 1 
//            PROPERTIES ( 
//                "replication_allocation" = "tag.location.default: 1"  
//         )
//         """
//     def insert_num = 5
//     for (int i = 0; i < insert_num; ++i) {
//         sql """
//                INSERT INTO ${dbName}.${tableName} VALUES (${i}, ${i})
//             """ 
//     }

//     res = sql "SELECT * FROM ${dbName}.${tableName}"
//     assertEquals(res.size(), insert_num)

//     sql "drop user if exists user1;"
//     sql "drop user if exists user2;"
//     sql "drop user if exists user3;"

//     sql "drop role if exists role_select;"
//     sql "drop role if exists role_load;"
//     sql "drop row policy if exists test_row_policy_1 on ${dbName}.${tableName};"


//     sql "CREATE USER 'user1' IDENTIFIED BY '12345' PASSWORD_EXPIRE INTERVAL 10 DAY FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 1 DAY;"
//     sql "CREATE USER 'user2' IDENTIFIED BY '12345' PASSWORD_EXPIRE INTERVAL 10 DAY FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 1 DAY;"
//     sql "CREATE USER 'user3' IDENTIFIED BY '12345' PASSWORD_EXPIRE INTERVAL 10 DAY FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 1 DAY;"

//     sql "create role role_select;"
//     sql "GRANT Select_priv ON *.* TO ROLE 'role_select';"

//     sql "create role role_load;"
//     sql "GRANT Load_priv ON *.* TO ROLE 'role_load';"

//     sql "grant 'role_select', 'role_load' to 'user1'@'%';"
//     sql "grant 'role_select' to 'user2'@'%';"
//     sql "grant 'role_load' to 'user3'@'%';"

//     sql "CREATE ROW POLICY test_row_policy_1 ON ${dbName}.${tableName} AS RESTRICTIVE TO user1 USING (id = 1);"


//     qt_sql_before_restore1 "show all grants;"
//     qt_sql_before_restore2 "show roles;"
//     //except 'password_policy.password_creation_time'
//     qt_sql_before_restore3 "select Host,User,Node_priv,Admin_priv,Grant_priv,Select_priv ,Load_priv,Alter_priv ,Create_priv,Drop_priv,Usage_priv,Show_view_priv,Cluster_usage_priv, Stage_usage_priv,ssl_type,ssl_cipher,x509_issuer,x509_subject,max_questions,max_updates,max_connections,max_user_connections,plugin,authentication_string,'password_policy.expiration_seconds','password_policy.history_num', 'password_policy.history_passwords','password_policy.num_failed_login','password_policy.password_lock_seconds', 'password_policy.failed_login_counter','password_policy.lock_time' from mysql.user;"
//     qt_sql_before_restore4 "show row policy;"

//     // check row policy valid
//     connect(user="user1", password="12345", url=url) {
//         try {
//             res = sql "SELECT * FROM ${dbName}.${tableName}"
//             assertEquals(res.size(), 1)
//         } catch (Exception e) {
//             log.info(e.getMessage())
//         }
//     }


//     sql """
//         BACKUP SNAPSHOT ${dbName}.${snapshotName}
//         TO `${repoName}`
//     """

//     syncer.waitSnapshotFinish(dbName)

//     sql "drop user if exists user1;"
//     sql "drop user if exists user2;"
//     sql "drop user if exists user3;"

//     sql "drop role if exists role_select;"
//     sql "drop role if exists role_load;"
//     sql "drop row policy if exists test_row_policy_1 on ${dbName}.${tableName};"
//     sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"


//     qt_sql_1 "show all grants;"
//     qt_sql_2 "show roles;"
//     //except 'password_policy.password_creation_time'
//     qt_sql_3 "select Host,User,Node_priv,Admin_priv,Grant_priv,Select_priv ,Load_priv,Alter_priv ,Create_priv,Drop_priv,Usage_priv,Show_view_priv,Cluster_usage_priv, Stage_usage_priv,ssl_type,ssl_cipher,x509_issuer,x509_subject,max_questions,max_updates,max_connections,max_user_connections,plugin,authentication_string,'password_policy.expiration_seconds','password_policy.history_num', 'password_policy.history_passwords','password_policy.num_failed_login','password_policy.password_lock_seconds', 'password_policy.failed_login_counter','password_policy.lock_time' from mysql.user;"
//     qt_sql_4 "show row policy;"


//     def snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
//     assertTrue(snapshot != null)

//     sql """
//         RESTORE SNAPSHOT ${dbName}.${snapshotName}
//         FROM `${repoName}`
//         PROPERTIES
//         (
//             "backup_timestamp" = "${snapshot}",
//             "reserve_replica" = "true"
//         )
//     """

//     syncer.waitAllRestoreFinish(dbName)

//     res = sql "SELECT * FROM ${dbName}.${tableName}"
//     assertEquals(res.size(), insert_num)

//     // check row policy valid
//     connect(user="user1", password="12345", url=url) {
//         try {
//             res = sql "SELECT * FROM ${dbName}.${tableName}"
//             assertEquals(res.size(), 1)
//         } catch (Exception e) {
//             log.info(e.getMessage())
//         }
//     }
 
//     qt_sql_after_restore1 "show all grants;"
//     qt_sql_after_restore2 "show roles;"
//     //except 'password_policy.password_creation_time'
//     qt_sql_after_restore3 "select Host,User,Node_priv,Admin_priv,Grant_priv,Select_priv ,Load_priv,Alter_priv ,Create_priv,Drop_priv,Usage_priv,Show_view_priv,Cluster_usage_priv, Stage_usage_priv,ssl_type,ssl_cipher,x509_issuer,x509_subject,max_questions,max_updates,max_connections,max_user_connections,plugin,authentication_string,'password_policy.expiration_seconds','password_policy.history_num', 'password_policy.history_passwords','password_policy.num_failed_login','password_policy.password_lock_seconds', 'password_policy.failed_login_counter','password_policy.lock_time' from mysql.user;"
//     qt_sql_after_restore4 "show row policy;"

//     sql "drop user if exists user1;"
//     sql "drop user if exists user2;"
//     sql "drop user if exists user3;"

//     sql "drop role if exists role_select;"
//     sql "drop role if exists role_load;"
//     sql "drop row policy if exists test_row_policy_1 on ${dbName}.${tableName};"

//     sql "DROP DATABASE ${dbName} FORCE"
//     sql "DROP REPOSITORY `${repoName}`"
// }


// suite("test_backup_restore_priv4", "backup_restore") {
//     String suiteName = "test_backup_restore_priv4"
//     String repoName = "${suiteName}_repo"
//     String dbName = "${suiteName}_db"
//     String tableName = "${suiteName}_table"
//     String snapshotName = "${suiteName}_snapshot"
//     def tokens = context.config.jdbcUrl.split('/')
//     def url=tokens[0] + "//" + tokens[2] + "/" + dbName + "?"

//     def syncer = getSyncer()
//     syncer.createS3Repository(repoName)

//     sql "DROP DATABASE IF EXISTS ${dbName}"
//     sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
//     sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
//     sql """
//            CREATE TABLE if NOT EXISTS ${dbName}.${tableName} 
//            (
//                `test` INT,
//                `id` INT
//            )
//            ENGINE=OLAP
//            UNIQUE KEY(`test`, `id`)
//            DISTRIBUTED BY HASH(id) BUCKETS 1 
//            PROPERTIES ( 
//                "replication_allocation" = "tag.location.default: 1"  
//         )
//         """
//     def insert_num = 5
//     for (int i = 0; i < insert_num; ++i) {
//         sql """
//                INSERT INTO ${dbName}.${tableName} VALUES (${i}, ${i})
//             """ 
//     }

//     res = sql "SELECT * FROM ${dbName}.${tableName}"
//     assertEquals(res.size(), insert_num)

//     sql "drop user if exists user1;"
//     sql "drop user if exists user2;"
//     sql "drop user if exists user3;"

//     sql "drop role if exists role_select;"
//     sql "drop role if exists role_load;"
//     sql "drop row policy if exists test_row_policy_1 on ${dbName}.${tableName};"
//     sql "drop catalog if exists mysql;"
//     sql "drop workload group if exists wg1;"
//     sql "drop workload group if exists wg2;"


//     sql "CREATE USER 'user1' IDENTIFIED BY '12345' PASSWORD_EXPIRE INTERVAL 10 DAY FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 1 DAY;"
//     sql "CREATE USER 'user2' IDENTIFIED BY '12345' PASSWORD_EXPIRE INTERVAL 10 DAY FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 1 DAY;"
//     sql "CREATE USER 'user3' IDENTIFIED BY '12345' PASSWORD_EXPIRE INTERVAL 10 DAY FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 1 DAY;"

//     sql "create role role_select;"
//     sql "GRANT Select_priv ON *.* TO ROLE 'role_select';"

//     sql "create role role_load;"
//     sql "GRANT Load_priv ON *.* TO ROLE 'role_load';"

//     sql "grant 'role_select', 'role_load' to 'user1'@'%';"
//     sql "grant 'role_select' to 'user2'@'%';"
//     sql "grant 'role_load' to 'user3'@'%';"

//     sql "CREATE ROW POLICY test_row_policy_1 ON ${dbName}.${tableName} AS RESTRICTIVE TO user1 USING (id = 1);"

//     sql """
//         CREATE CATALOG mysql PROPERTIES (
//         "type"="jdbc",
//         "user"="root",
//         "password"="",
//         "jdbc_url" = "jdbc:mysql://127.0.0.1:9030",
//         "driver_url" = "mysql-connector-j-8.0.31.jar",
//         "driver_class" = "com.mysql.cj.jdbc.Driver"
//         );
//     """

//     sql """ create workload group wg1 properties('tag'='cn1', "memory_limit"="45%"); """
//     sql """ create workload group wg2 properties ("max_concurrency"="5","max_queue_size" = "50"); """

//     qt_sql_before_restore1 "show all grants;"
//     qt_sql_before_restore2 "show roles;"
//     //except 'password_policy.password_creation_time'
//     qt_order_before_restore3 "select * except(`password_policy.password_creation_time`) from mysql.user order by host, user;"
//     qt_sql_before_restore4 "show row policy;"
//     qt_order_before_restore6 "select * except (id) from information_schema.workload_groups order by name;"
//     def result = sql "show catalogs;"
//     assertTrue(result.size() == 2)


//     // check row policy valid
//     connect(user="user1", password="12345", url=url) {
//         try {
//             res = sql "SELECT * FROM ${dbName}.${tableName}"
//             assertEquals(res.size(), 1)
//         } catch (Exception e) {
//             log.info(e.getMessage())
//         }
//     }


//     sql """
//         BACKUP SNAPSHOT ${dbName}.${snapshotName}
//         TO `${repoName}`
//     """

//     syncer.waitSnapshotFinish(dbName)

//     sql "drop user if exists user1;"
//     sql "drop user if exists user2;"
//     sql "drop user if exists user3;"

//     sql "drop role if exists role_select;"
//     sql "drop role if exists role_load;"
//     sql "drop row policy if exists test_row_policy_1 on ${dbName}.${tableName};"
//     sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
//     sql "drop catalog if exists mysql;"
//     sql "drop workload group if exists wg1;"
//     sql "drop workload group if exists wg2;"


//     qt_sql_1 "show all grants;"
//     qt_sql_2 "show roles;"
//     //except 'password_policy.password_creation_time'
//     qt_order_3 "select * except(`password_policy.password_creation_time`) from mysql.user order by host, user;"
//     qt_sql_4 "show row policy;"
//     qt_order_5 "select * except (id) from information_schema.workload_groups order by name;"
//     result = sql "show catalogs;"
//     assertTrue(result.size() == 1)

//     def snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
//     assertTrue(snapshot != null)

//     sql """
//         RESTORE SNAPSHOT ${dbName}.${snapshotName}
//         FROM `${repoName}`
//         PROPERTIES
//         (
//             "backup_timestamp" = "${snapshot}",
//             "reserve_replica" = "true"
//         )
//     """

//     syncer.waitAllRestoreFinish(dbName)

//     res = sql "SELECT * FROM ${dbName}.${tableName}"
//     assertEquals(res.size(), insert_num)

//     // check row policy valid
//     connect(user="user1", password="12345", url=url) {
//         try {
//             res = sql "SELECT * FROM ${dbName}.${tableName}"
//             assertEquals(res.size(), 1)
//         } catch (Exception e) {
//             log.info(e.getMessage())
//         }
//     }
 
//     qt_sql_after_restore1 "show all grants;"
//     qt_sql_after_restore2 "show roles;"
//     //except 'password_policy.password_creation_time'
//     qt_order_after_restore3 "select * except(`password_policy.password_creation_time`) from mysql.user order by host, user;"
//     qt_sql_after_restore4 "show row policy;"
//     qt_order_after_restore5 "select * except (id) from information_schema.workload_groups order by name;"
//     result = sql "show catalogs;"
//     assertTrue(result.size() == 2)

//     sql "drop user if exists user1;"
//     sql "drop user if exists user2;"
//     sql "drop user if exists user3;"

//     sql "drop role if exists role_select;"
//     sql "drop role if exists role_load;"
//     sql "drop row policy if exists test_row_policy_1 on ${dbName}.${tableName};"
//     sql "drop catalog if exists mysql;"
//     sql "drop workload group if exists wg1;"
//     sql "drop workload group if exists wg2;"

//     sql "DROP DATABASE ${dbName} FORCE"
//     sql "DROP REPOSITORY `${repoName}`"
// }



// suite("test_backup_restore_priv5", "backup_restore") {
//     String suiteName = "test_backup_restore_priv5"
//     String repoName = "${suiteName}_repo"
//     String dbName = "${suiteName}_db"
//     String tableName = "${suiteName}_table"
//     String snapshotName = "${suiteName}_snapshot"
//     def tokens = context.config.jdbcUrl.split('/')
//     def url=tokens[0] + "//" + tokens[2] + "/" + dbName + "?"

//     def syncer = getSyncer()
//     syncer.createS3Repository(repoName)

//     sql "DROP DATABASE IF EXISTS ${dbName}"
//     sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
//     sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
//     sql """
//            CREATE TABLE if NOT EXISTS ${dbName}.${tableName} 
//            (
//                `test` INT,
//                `id` INT
//            )
//            ENGINE=OLAP
//            UNIQUE KEY(`test`, `id`)
//            DISTRIBUTED BY HASH(id) BUCKETS 1 
//            PROPERTIES ( 
//                "replication_allocation" = "tag.location.default: 1"  
//         )
//         """
//     def insert_num = 5
//     for (int i = 0; i < insert_num; ++i) {
//         sql """
//                INSERT INTO ${dbName}.${tableName} VALUES (${i}, ${i})
//             """ 
//     }

//     res = sql "SELECT * FROM ${dbName}.${tableName}"
//     assertEquals(res.size(), insert_num)

//     sql "drop user if exists user1;"
//     sql "drop user if exists user2;"
//     sql "drop user if exists user3;"

//     sql "drop role if exists role_select;"
//     sql "drop role if exists role_load;"
//     sql "drop row policy if exists test_row_policy_1 on ${dbName}.${tableName};"
//     sql "drop catalog if exists mysql;"
//     sql "drop workload group if exists wg1;"
//     sql "drop workload group if exists wg2;"


//     sql "CREATE USER 'user1' IDENTIFIED BY '12345' PASSWORD_EXPIRE INTERVAL 10 DAY FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 1 DAY;"
//     sql "CREATE USER 'user2' IDENTIFIED BY '12345' PASSWORD_EXPIRE INTERVAL 10 DAY FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 1 DAY;"
//     sql "CREATE USER 'user3' IDENTIFIED BY '12345' PASSWORD_EXPIRE INTERVAL 10 DAY FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 1 DAY;"

//     sql "create role role_select;"
//     sql "GRANT Select_priv ON *.* TO ROLE 'role_select';"

//     sql "create role role_load;"
//     sql "GRANT Load_priv ON *.* TO ROLE 'role_load';"

//     sql "grant 'role_select', 'role_load' to 'user1'@'%';"
//     sql "grant 'role_select' to 'user2'@'%';"
//     sql "grant 'role_load' to 'user3'@'%';"

//     sql "CREATE ROW POLICY test_row_policy_1 ON ${dbName}.${tableName} AS RESTRICTIVE TO user1 USING (id = 1);"

//     sql """
//         CREATE CATALOG mysql PROPERTIES (
//         "type"="jdbc",
//         "user"="root",
//         "password"="",
//         "jdbc_url" = "jdbc:mysql://127.0.0.1:9030",
//         "driver_url" = "mysql-connector-j-8.0.31.jar",
//         "driver_class" = "com.mysql.cj.jdbc.Driver"
//         );
//     """

//     sql """ create workload group wg1 properties('tag'='cn1', "memory_limit"="45%"); """
//     sql """ create workload group wg2 properties ("max_concurrency"="5","max_queue_size" = "50"); """

//     qt_sql_before_restore1 "show all grants;"
//     qt_sql_before_restore2 "show roles;"
//     //except 'password_policy.password_creation_time'
//     qt_order_before_restore3 "select * except(`password_policy.password_creation_time`) from mysql.user order by host, user;"
//     qt_sql_before_restore4 "show row policy;"
//     qt_order_before_restore6 "select * except (id) from information_schema.workload_groups order by name;"
//     def result = sql "show catalogs;"
//     assertTrue(result.size() == 2)


//     // check row policy valid
//     connect(user="user1", password="12345", url=url) {
//         try {
//             res = sql "SELECT * FROM ${dbName}.${tableName}"
//             assertEquals(res.size(), 1)
//         } catch (Exception e) {
//             log.info(e.getMessage())
//         }
//     }


//     sql """
//         BACKUP GLOBAL SNAPSHOT ${snapshotName}
//         TO `${repoName}`
//     """

//     syncer.waitSnapshotFinish("__internal_schema")

//     sql "drop user if exists user1;"
//     sql "drop user if exists user2;"
//     sql "drop user if exists user3;"

//     sql "drop role if exists role_select;"
//     sql "drop role if exists role_load;"
//     sql "drop row policy if exists test_row_policy_1 on ${dbName}.${tableName};"
//     sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
//     sql "drop catalog if exists mysql;"
//     sql "drop workload group if exists wg1;"
//     sql "drop workload group if exists wg2;"


//     qt_sql_1 "show all grants;"
//     qt_sql_2 "show roles;"
//     //except 'password_policy.password_creation_time'
//     qt_order_3 "select * except(`password_policy.password_creation_time`) from mysql.user order by host, user;"
//     qt_sql_4 "show row policy;"
//     qt_order_5 "select * except (id) from information_schema.workload_groups order by name;"
//     result = sql "show catalogs;"
//     assertTrue(result.size() == 1)

//     def snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
//     assertTrue(snapshot != null)

//     sql """
//         RESTORE GLOBAL SNAPSHOT ${snapshotName}
//         FROM `${repoName}`
//         PROPERTIES
//         (
//             "backup_timestamp" = "${snapshot}",
//             "reserve_replica" = "true"
//         )
//     """

//     syncer.waitAllRestoreFinish("__internal_schema")

//     test {
//         sql "SELECT * FROM ${dbName}.${tableName}"
//         exception "does not exist in database"
//     }

//     // check row policy valid
//     connect(user="user1", password="12345", url=url) {
//         try {
//             res = sql "SELECT * FROM ${dbName}.${tableName}"
//             assertEquals(res.size(), 1)
//         } catch (Exception e) {
//             log.info(e.getMessage())
//         }
//     }
 
//     qt_sql_after_restore1 "show all grants;"
//     qt_sql_after_restore2 "show roles;"
//     //except 'password_policy.password_creation_time'
//     qt_order_after_restore3 "select * except(`password_policy.password_creation_time`) from mysql.user order by host, user;"
//     qt_sql_after_restore4 "show row policy;"
//     qt_order_after_restore5 "select * except (id) from information_schema.workload_groups order by name;"
//     result = sql "show catalogs;"
//     assertTrue(result.size() == 2)

//     sql "drop user if exists user1;"
//     sql "drop user if exists user2;"
//     sql "drop user if exists user3;"

//     sql "drop role if exists role_select;"
//     sql "drop role if exists role_load;"
//     sql "drop row policy if exists test_row_policy_1 on ${dbName}.${tableName};"
//     sql "drop catalog if exists mysql;"
//     sql "drop workload group if exists wg1;"
//     sql "drop workload group if exists wg2;"

//     sql "DROP DATABASE ${dbName} FORCE"
//     sql "DROP REPOSITORY `${repoName}`"
// }


suite("test_backup_restore_priv", "backup_restore") {
    String suiteName = "test_backup_restore_priv"
    String repoName = "${suiteName}_repo"
    String dbName = "${suiteName}_db"
    String tableName = "${suiteName}_table"
    String snapshotName = "${suiteName}_snapshot"
    def tokens = context.config.jdbcUrl.split('/')
    def url=tokens[0] + "//" + tokens[2] + "/" + dbName + "?"

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)

    sql "DROP DATABASE IF EXISTS ${dbName}"
    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    sql """
           CREATE TABLE if NOT EXISTS ${dbName}.${tableName}
           (
               `test` INT,
               `id` INT
           )
           ENGINE=OLAP
           UNIQUE KEY(`test`, `id`)
           DISTRIBUTED BY HASH(id) BUCKETS 1
           PROPERTIES (
               "replication_allocation" = "tag.location.default: 1"
        )
        """
    def insert_num = 5
    for (int i = 0; i < insert_num; ++i) {
        sql """
               INSERT INTO ${dbName}.${tableName} VALUES (${i}, ${i})
            """
    }

    res = sql "SELECT * FROM ${dbName}.${tableName}"
    assertEquals(res.size(), insert_num)

    sql "drop user if exists user1;"
    sql "drop user if exists user2;"
    sql "drop user if exists user3;"

    sql "drop role if exists role_select;"
    sql "drop role if exists role_load;"
    sql "drop row policy if exists test_row_policy_1 on ${dbName}.${tableName};"
    sql "drop catalog if exists mysql;"
    sql "drop workload group if exists wg1;"
    sql "drop workload group if exists wg2;"


    sql "CREATE USER 'user1' IDENTIFIED BY '12345' PASSWORD_EXPIRE INTERVAL 10 DAY FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 1 DAY;"
    sql "CREATE USER 'user2' IDENTIFIED BY '12345' PASSWORD_EXPIRE INTERVAL 10 DAY FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 1 DAY;"
    sql "CREATE USER 'user3' IDENTIFIED BY '12345' PASSWORD_EXPIRE INTERVAL 10 DAY FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 1 DAY;"

    sql "create role role_select;"
    sql "GRANT Select_priv ON *.* TO ROLE 'role_select';"

    sql "create role role_load;"
    sql "GRANT Load_priv ON *.* TO ROLE 'role_load';"

    sql "grant 'role_select', 'role_load' to 'user1'@'%';"
    sql "grant 'role_select' to 'user2'@'%';"
    sql "grant 'role_load' to 'user3'@'%';"

    sql "CREATE ROW POLICY test_row_policy_1 ON ${dbName}.${tableName} AS RESTRICTIVE TO user1 USING (id = 1);"

    sql """
        CREATE CATALOG mysql PROPERTIES (
        "type"="jdbc",
        "user"="root",
        "password"="",
        "jdbc_url" = "jdbc:mysql://127.0.0.1:9030",
        "driver_url" = "mysql-connector-j-8.0.31.jar",
        "driver_class" = "com.mysql.cj.jdbc.Driver"
        );
    """

    sql """ create workload group wg1 properties('tag'='cn1', "memory_limit"="45%"); """
    sql """ create workload group wg2 properties ("max_concurrency"="5","max_queue_size" = "50"); """

    qt_sql_before_restore1 "show all grants;"
    qt_sql_before_restore2 "show roles;"
    //except 'password_policy.password_creation_time'
    qt_order_before_restore3 "select * except(`password_policy.password_creation_time`) from mysql.user order by host, user;"
    qt_sql_before_restore4 "show row policy;"
    qt_order_before_restore6 "select * except (id) from information_schema.workload_groups order by name;"
    def result = sql "show catalogs;"
    assertTrue(result.size() == 2)


    // check row policy valid
    connect(user="user1", password="12345", url=url) {
        try {
            res = sql "SELECT * FROM ${dbName}.${tableName}"
            assertEquals(res.size(), 1)
        } catch (Exception e) {
            log.info(e.getMessage())
        }
    }


    sql """
        BACKUP GLOBAL SNAPSHOT ${snapshotName}
        TO `${repoName}`
        PROPERTIES (
            "backup_privilege"="true",
            "backup_catalog"="true",
            "backup_workload_group"="true"
        )
    """

    syncer.waitSnapshotFinish("__internal_schema")

    sql "drop user if exists user1;"
    sql "drop user if exists user2;"
    sql "drop user if exists user3;"

    sql "drop role if exists role_select;"
    sql "drop role if exists role_load;"
    sql "drop row policy if exists test_row_policy_1 on ${dbName}.${tableName};"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    sql "drop catalog if exists mysql;"
    sql "drop workload group if exists wg1;"
    sql "drop workload group if exists wg2;"


    qt_sql_1 "show all grants;"
    qt_sql_2 "show roles;"
    //except 'password_policy.password_creation_time'
    qt_order_3 "select * except(`password_policy.password_creation_time`) from mysql.user order by host, user;"
    qt_sql_4 "show row policy;"
    qt_order_5 "select * except (id) from information_schema.workload_groups order by name;"
    result = sql "show catalogs;"
    assertTrue(result.size() == 1)

    def snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
    assertTrue(snapshot != null)

    logger.info(""" ======================================  1 "reserve_privilege"="true", "reserve_catalog"="true","reserve_workload_group"="true" ==================================== """)

    sql """
        RESTORE GLOBAL SNAPSHOT ${snapshotName}
        FROM `${repoName}`
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "reserve_privilege"="true",
            "reserve_catalog"="true",
            "reserve_workload_group"="true"
        )
    """

    syncer.waitAllRestoreFinish("__internal_schema")

    test {
        sql "SELECT * FROM ${dbName}.${tableName}"
        exception "does not exist in database"
    }
 
    qt_sql_after_restore1 "show all grants;"
    qt_sql_after_restore2 "show roles;"
    //except 'password_policy.password_creation_time'
    qt_order_after_restore3 "select * except(`password_policy.password_creation_time`) from mysql.user order by host, user;"
    qt_sql_after_restore4 "show row policy;"
    qt_order_after_restore5 "select * except (id) from information_schema.workload_groups order by name;"
    result = sql "show catalogs;"
    assertTrue(result.size() == 2)

    logger.info(" ====================================== 2 without reserve ==================================== ")
    sql "drop user if exists user1;"
    sql "drop user if exists user2;"
    sql "drop user if exists user3;"

    sql "drop role if exists role_select;"
    sql "drop role if exists role_load;"
    sql "drop row policy if exists test_row_policy_1 on ${dbName}.${tableName};"
    sql "drop catalog if exists mysql;"
    sql "drop workload group if exists wg1;"
    sql "drop workload group if exists wg2;"

    sql """
        RESTORE GLOBAL SNAPSHOT ${snapshotName}
        FROM `${repoName}`
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true"
        )
    """

    syncer.waitAllRestoreFinish("__internal_schema")

    test {
        sql "SELECT * FROM ${dbName}.${tableName}"
        exception "does not exist in database"
    }
 
    qt_sql_after_restore1 "show all grants;"
    qt_sql_after_restore2 "show roles;"
    //except 'password_policy.password_creation_time'
    qt_order_after_restore3 "select * except(`password_policy.password_creation_time`) from mysql.user order by host, user;"
    qt_sql_after_restore4 "show row policy;"
    qt_order_after_restore5 "select * except (id) from information_schema.workload_groups order by name;"
    result = sql "show catalogs;"
    assertTrue(result.size() == 2)


    logger.info(""" ======================================  3 "reserve_privilege"="true" ==================================== """)
    sql "drop user if exists user1;"
    sql "drop user if exists user2;"
    sql "drop user if exists user3;"

    sql "drop role if exists role_select;"
    sql "drop role if exists role_load;"
    sql "drop row policy if exists test_row_policy_1 on ${dbName}.${tableName};"
    sql "drop catalog if exists mysql;"
    sql "drop workload group if exists wg1;"
    sql "drop workload group if exists wg2;"

    sql """
        RESTORE GLOBAL SNAPSHOT ${snapshotName}
        FROM `${repoName}`
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "reserve_privilege"="true"
        )
    """

    syncer.waitAllRestoreFinish("__internal_schema")

    test {
        sql "SELECT * FROM ${dbName}.${tableName}"
        exception "does not exist in database"
    }
 
    qt_sql_after_restore1 "show all grants;"
    qt_sql_after_restore2 "show roles;"
    //except 'password_policy.password_creation_time'
    qt_order_after_restore3 "select * except(`password_policy.password_creation_time`) from mysql.user order by host, user;"
    qt_sql_after_restore4 "show row policy;"
    qt_order_after_restore5 "select * except (id) from information_schema.workload_groups order by name;"
    result = sql "show catalogs;"
    assertTrue(result.size() == 1)


    logger.info(""" ======================================  4 "reserve_catalog"="true" ==================================== """)
    sql "drop user if exists user1;"
    sql "drop user if exists user2;"
    sql "drop user if exists user3;"

    sql "drop role if exists role_select;"
    sql "drop role if exists role_load;"
    sql "drop row policy if exists test_row_policy_1 on ${dbName}.${tableName};"
    sql "drop catalog if exists mysql;"
    sql "drop workload group if exists wg1;"
    sql "drop workload group if exists wg2;"

    sql """
        RESTORE GLOBAL SNAPSHOT ${snapshotName}
        FROM `${repoName}`
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "reserve_catalog"="true"
        )
    """

    syncer.waitAllRestoreFinish("__internal_schema")

    test {
        sql "SELECT * FROM ${dbName}.${tableName}"
        exception "does not exist in database"
    }
 
    qt_sql_after_restore1 "show all grants;"
    qt_sql_after_restore2 "show roles;"
    //except 'password_policy.password_creation_time'
    qt_order_after_restore3 "select * except(`password_policy.password_creation_time`) from mysql.user order by host, user;"
    qt_sql_after_restore4 "show row policy;"
    qt_order_after_restore5 "select * except (id) from information_schema.workload_groups order by name;"
    result = sql "show catalogs;"
    assertTrue(result.size() == 2)



    logger.info(""" ======================================  5 "reserve_workload_group"="true" ==================================== """)
    sql "drop user if exists user1;"
    sql "drop user if exists user2;"
    sql "drop user if exists user3;"

    sql "drop role if exists role_select;"
    sql "drop role if exists role_load;"
    sql "drop row policy if exists test_row_policy_1 on ${dbName}.${tableName};"
    sql "drop catalog if exists mysql;"
    sql "drop workload group if exists wg1;"
    sql "drop workload group if exists wg2;"

    sql """
        RESTORE GLOBAL SNAPSHOT ${snapshotName}
        FROM `${repoName}`
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "reserve_workload_group"="true"
        )
    """

    syncer.waitAllRestoreFinish("__internal_schema")

    test {
        sql "SELECT * FROM ${dbName}.${tableName}"
        exception "does not exist in database"
    }
 
    qt_sql_after_restore1 "show all grants;"
    qt_sql_after_restore2 "show roles;"
    //except 'password_policy.password_creation_time'
    qt_order_after_restore3 "select * except(`password_policy.password_creation_time`) from mysql.user order by host, user;"
    qt_sql_after_restore4 "show row policy;"
    qt_order_after_restore5 "select * except (id) from information_schema.workload_groups order by name;"
    result = sql "show catalogs;"
    assertTrue(result.size() == 1)


    logger.info(""" ======================================  6 "reserve_privilege"="true","reserve_workload_group"="true" ==================================== """)

    sql "drop user if exists user1;"
    sql "drop user if exists user2;"
    sql "drop user if exists user3;"

    sql "drop role if exists role_select;"
    sql "drop role if exists role_load;"
    sql "drop row policy if exists test_row_policy_1 on ${dbName}.${tableName};"
    sql "drop catalog if exists mysql;"
    sql "drop workload group if exists wg1;"
    sql "drop workload group if exists wg2;"

    sql """
        RESTORE GLOBAL SNAPSHOT ${snapshotName}
        FROM `${repoName}`
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "reserve_privilege"="true",
            "reserve_workload_group"="true"
        )
    """

    syncer.waitAllRestoreFinish("__internal_schema")

    test {
        sql "SELECT * FROM ${dbName}.${tableName}"
        exception "does not exist in database"
    }
 
    qt_sql_after_restore1 "show all grants;"
    qt_sql_after_restore2 "show roles;"
    //except 'password_policy.password_creation_time'
    qt_order_after_restore3 "select * except(`password_policy.password_creation_time`) from mysql.user order by host, user;"
    qt_sql_after_restore4 "show row policy;"
    qt_order_after_restore5 "select * except (id) from information_schema.workload_groups order by name;"
    result = sql "show catalogs;"
    assertTrue(result.size() == 1)

 

    //cleanup
    sql "drop user if exists user1;"
    sql "drop user if exists user2;"
    sql "drop user if exists user3;"

    sql "drop role if exists role_select;"
    sql "drop role if exists role_load;"
    sql "drop row policy if exists test_row_policy_1 on ${dbName}.${tableName};"
    sql "drop catalog if exists mysql;"
    sql "drop workload group if exists wg1;"
    sql "drop workload group if exists wg2;"

    sql "DROP DATABASE ${dbName} FORCE"
    sql "DROP REPOSITORY `${repoName}`"
}