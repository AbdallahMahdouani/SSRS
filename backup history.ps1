<#
  Mahdouani Abdallah @2020
#>

Import-Module "SQLPS" -DisableNameChecking
Add-Type -AssemblyName "Microsoft.SqlServer.Smo"
Add-Type -AssemblyName "Microsoft.SqlServer.SmoExtended"

cd c:

$CollectServer = "DESKTOP-95KKMMH"
$collectDatabase = "msdb"

$serverList ="DESKTOP-95KKMMH","DESKTOP-95KKMMH\production"

foreach($svr in $serverList)
{
    $servername = $svr
    
    $sqlRec =";WITH LAST_FULL_BACKUP_LIST
AS
(
SELECT @@servername as servername
,SYSDBLIST.name AS Name,
MAX(BUSETS.backup_finish_date) AS Last_Backup_Finish_DateTime
FROM 
MASTER.sys.databases AS SYSDBLIST
LEFT OUTER JOIN
msdb.dbo.backupset AS BUSETS
ON
SYSDBLIST.name = BUSETS.database_name
WHERE
SYSDBLIST.name<>'TempDB'
AND
BUSETS.[type] ='D' OR BUSETS.[type] IS NULL
GROUP BY 
SYSDBLIST.name
)

,
---Get List of Last Differential Backup
LAST_DIFFERENTIAL_BACKUP_LIST
AS
(
SELECT
SYSDBLIST.name AS Name,
MAX(BUSETS.backup_finish_date) AS Last_Backup_Finish_DateTime
FROM 
MASTER.sys.databases AS SYSDBLIST
LEFT OUTER JOIN
msdb.dbo.backupset AS BUSETS
ON
SYSDBLIST.name = BUSETS.database_name
WHERE
SYSDBLIST.name<>'TempDB'
AND
BUSETS.[type] ='I' 
GROUP BY 
SYSDBLIST.name
)

,
---Get List of Last Log Backup

LOG_BACKUP_LIST
AS
(
SELECT
SYSDBLIST.name AS Name,
BUSETS.backup_finish_date AS Backup_Finish_DateTime,
ROUND(((BUSETS.backup_size/1024)/1024),2) AS Backup_Size_MB,
ROW_NUMBER() OVER(Partition by SYSDBLIST.name ORDER BY BUSETS.backup_finish_date DESC) AS RevOrderBuDate
FROM 
MASTER.sys.databases AS SYSDBLIST
LEFT OUTER JOIN
msdb.dbo.backupset AS BUSETS
ON
SYSDBLIST.name = BUSETS.database_name
WHERE
SYSDBLIST.name<>'TempDB'
AND
BUSETS.[type] ='L' 
)
,
LAST_LOG_BACKUP_LIST
AS
(SELECT
Name,
Backup_Finish_DateTime AS Last_Backup_Finish_DateTime,
Backup_Size_MB AS Log_Backup_Size_MB
FROM
LOG_BACKUP_LIST
WHERE
RevOrderBuDate=1
)



SELECT SERVERPROPERTY('Servername') AS ServerName,
SYSDBLIST.name AS Database_Name,
SYSDBLIST.Compatibility_level,
SYSDBLIST.Recovery_model,
SYSDBLIST.Recovery_model_desc,
BUSETS.database_creation_date AS Database_Create_Date,
CASE WHEN BUSETS.backup_set_id IS NULL THEN 'NO Backup' ELSE 'Backup Complete' END AS Backup_Completed,
BUSETS.backup_start_date AS Backup_Start_DateTime,
BUSETS.backup_finish_date AS Backup_Finish_DateTime,
DATEDIFF(MINUTE, BUSETS.backup_start_date, BUSETS.backup_finish_date) AS Duration_Min,
(DATEDIFF(DAY,BUSETS.backup_finish_date,GETDATE())) AS Days_Since_Last_Backup,

LASTDIFFBACKUP.Last_Backup_Finish_DateTime AS Last_Differential_Finish_DateTime,
(DATEDIFF(DAY,LASTDIFFBACKUP.Last_Backup_Finish_DateTime ,GETDATE())) AS Days_Since_Last_Differential_Backup,
LASTLOGBACKUP.Last_Backup_Finish_DateTime AS Last_Log_Finish_DateTime,
(DATEDIFF(DAY,LASTLOGBACKUP.Last_Backup_Finish_DateTime,GETDATE()))AS Days_Since_Last_Log_Backup,
cast(LASTLOGBACKUP.Log_Backup_Size_MB as decimal(10,2)) AS LOG_Backup_Size_MB,
CASE 
WHEN BUSETS.[type] = 'D' THEN 'Full Backup' 
WHEN BUSETS.[type] = 'I' THEN 'Differential Database' 
WHEN BUSETS.[type] = 'L' THEN 'Log' 
WHEN BUSETS.[type] = 'F' THEN 'File/Filegroup'
WHEN BUSETS.[type] = 'G' THEN 'Differential File'
WHEN BUSETS.[type] = 'P' THEN 'Partial'  
WHEN BUSETS.[type] = 'Q'THEN 'Differential partial' 
END AS Backup_Type,
cast(((BUSETS.backup_size/1024)/1024) as decimal(10,2)) AS Backup_Size_MB,
cast (((BUSETS.compressed_backup_size/1024)/1024) as decimal(10,2)) AS Backup_Size_Compressed_MB,
BUMEDFAM.Device_type,
BUMEDFAM.Physical_device_name,
BUMEDFAM.Logical_device_name
FROM 
MASTER.sys.databases AS SYSDBLIST
LEFT OUTER JOIN
msdb.dbo.backupset AS BUSETS
ON
SYSDBLIST.name = BUSETS.database_name
LEFT OUTER JOIN
msdb.dbo.backupmediafamily  AS BUMEDFAM
ON
BUSETS.media_set_id = BUMEDFAM.media_set_id

INNER JOIN
LAST_FULL_BACKUP_LIST AS LASTBACKUP
ON
SYSDBLIST.Name=LASTBACKUP.Name
AND
ISNULL(BUSETS.backup_finish_date,'01/01/1900') = ISNULL(LASTBACKUP. Last_Backup_Finish_DateTime,'01/01/1900')

LEFT OUTER JOIN
LAST_DIFFERENTIAL_BACKUP_LIST AS LASTDIFFBACKUP
ON
SYSDBLIST.Name=LASTDIFFBACKUP.Name

LEFT OUTER JOIN
LAST_LOG_BACKUP_LIST AS LASTLOGBACKUP
ON
SYSDBLIST.Name=LASTLOGBACKUP.Name
WHERE
SYSDBLIST.name<>'TempDB'
GO


" 
    $result = Invoke-Sqlcmd -ErrorAction 'Stop' -ServerInstance $servername -query $sqlRec
    foreach($line in $result){
                                      $insertReq =[String]::format(
                                      "insert into msdb.dbo.backup_history
                                              (ServerName ,
                                               Database_Name ,
                                               Compatibility_level ,
                                               Recovery_model ,
                                               Recovery_model_desc ,
                                               Database_Create_Date ,
                                               Backup_Completed ,
                                               Backup_Start_DateTime ,
                                               Backup_Finish_DateTime ,
                                               Duration_Min ,
                                               Days_Since_Last_Backup ,
                                               Last_Differential_Finish_DateTime,
                                               Days_Since_Last_Differential_Backup,
                                               Last_Log_Finish_DateTime ,
                                               Days_Since_Last_Log_Backup ,
                                               LOG_Backup_Size_MB,
                                               Backup_Type ,
                                               Backup_Size_MB ,
                                               Backup_Size_Compressed_MB, 
                                               Physical_device_name,
                                               Logical_device_name
                                               )
                                              
                                              values
                                           ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','{14}','{15}','{16}','{17}','{18}','{19}','{20}')"
                                      
                                      ,@{$true="NULL";$false=($line.ServerName).ToString()}[$line.ServerName-eq [System.DBNull]::Value]
                                      ,@{$true="NULL";$false=(($line.Database_Name ).ToString()).Replace(",",".")}[$line.Database_Name  -eq [System.DBNull]::Value]
                                      ,@{$true="NULL";$false=(($line.Compatibility_level).ToString()).Replace(",",".")}[$line.Compatibility_level -eq [System.DBNull]::Value]
                                      ,@{$true="NULL";$false=(($line.Recovery_model).ToString()).Replace(",",".")}[$line.Recovery_model -eq [System.DBNull]::Value]
                                      ,@{$true="NULL";$false=(($line.Recovery_model_desc).ToString()).Replace(",",".")}[$line.Recovery_model_desc -eq [System.DBNull]::Value]
                                      ,@{$true="NULL";$false=($line.Database_Create_Date).ToString().Replace("'","")}[$line.Database_Create_Date -eq [System.DBNull]::Value]
                                      ,@{$true="NULL";$false=($line.Backup_Completed).ToString()}[$line.Backup_Completed -eq [System.DBNull]::Value]
                                      ,@{$true="NULL";$false=($line.Backup_Start_DateTime).ToString()}[$line.Backup_Start_DateTime -eq [System.DBNull]::Value]
                                      ,@{$true="NULL";$false=($line.Backup_Finish_DateTime).ToString()}[$line.Backup_Finish_DateTime -eq [System.DBNull]::Value]
                                      ,@{$true="NULL";$false=($line.Duration_Min).ToString()}[$line.Duration_Min -eq [System.DBNull]::Value]
                                      ,@{$true="NULL";$false=($line.Days_Since_Last_Backup).ToString()}[$line.Days_Since_Last_Backup -eq [System.DBNull]::Value]
                                      ,@{$true="NULL";$false=($line.Last_Differential_Finish_DateTime).ToString()}[$line.Last_Differential_Finish_DateTime -eq [System.DBNull]::Value]
                                      ,@{$true="NULL";$false=($line.Days_Since_Last_Differential_Backup).ToString()}[$line.Days_Since_Last_Differential_Backup -eq [System.DBNull]::Value]
                                      ,@{$true="NULL";$false=($line.Last_Log_Finish_DateTime).ToString()}[$line.Last_Log_Finish_DateTime -eq [System.DBNull]::Value]
                                      ,@{$true="NULL";$false=($line.Days_Since_Last_Log_Backup).ToString()}[$line.Days_Since_Last_Log_Backup -eq [System.DBNull]::Value]
                                      ,@{$true="NULL";$false=($line.LOG_Backup_Size_MB).ToString()}[$line.LOG_Backup_Size_MB -eq [System.DBNull]::Value]
                                      ,@{$true="NULL";$false=($line.Backup_Type).ToString()}[$line.Backup_Type -eq [System.DBNull]::Value]
                                      ,@{$true="NULL";$false=($line.Backup_Size_MB).ToString()}[$line.Backup_Size_MB -eq [System.DBNull]::Value]
                                      ,@{$true="NULL";$false=($line.Backup_Size_Compressed_MB).ToString()}[$line.Backup_Size_Compressed_MB -eq [System.DBNull]::Value]
                                      ,@{$true="NULL";$false=($line.Backup_Size_Compressed_MB).ToString()}[$line.Backup_Size_Compressed_MB -eq [System.DBNull]::Value]
                                      ,@{$true="NULL";$false=($line.Physical_device_name).ToString()}[$line.Physical_device_name -eq [System.DBNull]::Value]
                                      ,@{$true="NULL";$false=($line.Logical_device_name).ToString()}[$line.Device_type -eq [System.DBNull]::Value]
                                      
                                      
                                      )
            $insertReq = $insertReq.Replace("'NULL'","NULL")
            Invoke-Sqlcmd -ErrorAction 'Stop' -ServerInstance $CollectServer -Database $collectDatabase -query $insertReq
            }
        }
 