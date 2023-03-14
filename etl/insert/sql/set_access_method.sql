SET LOCAL citus.multi_shard_modify_mode TO 'sequential';
SELECT alter_table_set_access_method('{TABLE_NAME}', '{ACCESS_METHOD}');