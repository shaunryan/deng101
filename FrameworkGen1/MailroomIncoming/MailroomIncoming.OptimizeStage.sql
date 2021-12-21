-- Databricks notebook source
-- MAGIC %sql
-- MAGIC 
-- MAGIC optimize MailroomIncoming.iautoswitchoptoutrequested;
-- MAGIC optimize MailroomIncoming.icancelledswitchresolutionfound;
-- MAGIC optimize MailroomIncoming.icoolingoffperiodexpired;
-- MAGIC optimize MailroomIncoming.ilikelysavingsopportunityfound;
-- MAGIC optimize MailroomIncoming.imanualenergyswitchrequested;
-- MAGIC optimize MailroomIncoming.isavingsopportunityrejected;
-- MAGIC optimize MailroomIncoming.isupplierrequestedswitchcancelled;
-- MAGIC optimize MailroomIncoming.iswitchcompleted;
-- MAGIC optimize MailroomIncoming.iswitchcompletionunconfirmed;
-- MAGIC optimize MailroomIncoming.iswitchdetailssenttosupplier;
-- MAGIC optimize MailroomIncoming.iswitchexpired;
-- MAGIC optimize MailroomIncoming.iswitchstarted;
-- MAGIC optimize MailroomIncoming.iuserrequestedswitchcancelfailed;
-- MAGIC optimize MailroomIncoming.iuserrequestedswitchcancellation;
-- MAGIC optimize MailroomIncoming.iuserrequestedswitchcancelled;
-- MAGIC optimize MailroomIncoming.preferences_iupdated;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC vacuum MailroomIncoming.iautoswitchoptoutrequested;
-- MAGIC vacuum MailroomIncoming.icancelledswitchresolutionfound;
-- MAGIC vacuum MailroomIncoming.icoolingoffperiodexpired;
-- MAGIC vacuum MailroomIncoming.ilikelysavingsopportunityfound;
-- MAGIC vacuum MailroomIncoming.imanualenergyswitchrequested;
-- MAGIC vacuum MailroomIncoming.isavingsopportunityrejected;
-- MAGIC vacuum MailroomIncoming.isupplierrequestedswitchcancelled;
-- MAGIC vacuum MailroomIncoming.iswitchcompleted;
-- MAGIC vacuum MailroomIncoming.iswitchcompletionunconfirmed;
-- MAGIC vacuum MailroomIncoming.iswitchdetailssenttosupplier;
-- MAGIC vacuum MailroomIncoming.iswitchexpired;
-- MAGIC vacuum MailroomIncoming.iswitchstarted;
-- MAGIC vacuum MailroomIncoming.iuserrequestedswitchcancelfailed;
-- MAGIC vacuum MailroomIncoming.iuserrequestedswitchcancellation;
-- MAGIC vacuum MailroomIncoming.iuserrequestedswitchcancelled;
-- MAGIC vacuum MailroomIncoming.preferences_iupdated;
