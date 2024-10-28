/*
Typically, you would use a role of ACCOUNTADMIN or SECURITYADMIN to manage
access control.  For the purposes of this lab, I have granted your roles 
elevated privileges.

Before a role can be used for access control, at least one user must be assigned to it. So let's 
create a new role named JUNIOR_DBA and assign it to your Snowflake user. To complete this task, you need 
to know your username, which is the name you used to log in to the UI.

If you try to perform this operation while in a role such as SYSADMIN, it would fail due to insufficient 
privileges. By default (and design), the SYSADMIN role cannot create new roles or users.
 */
CREATE ROLE <your_initials>_junior_dba;
GRANT ROLE <your_initials>_junior_dba TO USER YOUR_USERNAME_GOES_HERE;

use role <your_initials>_junior_dba;

/*
Note that the warehouse is not selected in the top right.  This newly created role has not been granted
access to any warehouses.  Let's fix that.

The junior DBA role does not have permissions to grant this access.  Execute the statement below and you 
will receive an error.  Switching back to your default role allows you to grant privs.

Once again, your role has been elevated to grant these privileges.  This would typically be an accountadmin
or securityadmin
 */
grant usage on warehouse <your_initials>_compute_wh to role <your_initials>_junior_dba;

use role <your_default_role>;
grant usage on warehouse <your_initials>_compute_wh to role <your_initials>_junior_dba;

use role <your_initials>_junior_dba;
use warehouse <your_initials>_compute_wh;

/*
Switch back to your default role and grant the JUNIOR_DBA the USAGE privilege required to view and use the 
your database and Financial__Economic_Essentials databases. Note that the Cybersyn database from the Marketplace 
uses GRANT IMPORTED PRIVILEGES, instead of GRANT USAGE.
 */

use role <your_default_role>;
GRANT USAGE ON DATABASE <your_db> TO ROLE <your_initials>_junior_dba;

GRANT IMPORTED PRIVILEGES ON DATABASE Financial__Economic_Essentials TO ROLE <your_initials>_junior_dba;

use role <your_initials>_junior_dba;

