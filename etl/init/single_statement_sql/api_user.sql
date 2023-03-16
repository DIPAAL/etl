-- Create user API if not exists. This is required as no CREATE USER IF NOT EXISTS exists in PostgreSQL 15.
do
$body$
declare
  num_users integer;
begin
   SELECT count(*)
     into num_users
   FROM pg_user
   WHERE usename = 'api'; -- No this is not a typo.

   IF num_users = 0 THEN
      CREATE ROLE api;
   END IF;
end
$body$
;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO api;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO api;