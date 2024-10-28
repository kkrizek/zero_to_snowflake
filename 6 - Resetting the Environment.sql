/*
Switch to a role with account level permissions and remove the account level objects that 
you created as a part of this lab. 
 */
USE ROLE <your_default_role>;

DROP WAREHOUSE IF EXISTS <your_initials>_analytics_wh;
DROP WAREHOUSE IF EXISTS <your_initials>_compute_wh;
DROP ROLE IF EXISTS <your_initials>_junior_dba;

