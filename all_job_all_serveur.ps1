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
    
    $sqlRec ="select @@servername as servername 
                     ,job.name
                     ,run_time
                     ,STUFF(
STUFF(STUFF(RIGHT(REPLICATE('0', 8) + CAST(run_duration AS VARCHAR(8)), 8), 3, 0, ':'), 6, 0, ':'),
9,
0,
':'
) 'duration'
,[message]
,run_status
,enabled
,run_date
,run_duration
,AVG(history.run_duration) OVER (PARTITION BY history.job_id) AS average_rt
,MAX(history.run_duration) OVER (PARTITION BY history.job_id) AS max_rt
,MIN(history.run_duration) OVER (PARTITION BY history.job_id) AS min_rt
,COUNT(history.job_id) OVER (PARTITION BY history.job_id, run_date) AS execution_day
,DATEPART(HOUR, dbo.agent_datetime(run_date, run_time)) AS Hour_day
,COUNT(history.job_id) OVER (PARTITION BY history.job_id
,DATEPART(HOUR, dbo.agent_datetime(run_date, run_time))) AS execution_hour
FROM sysjobhistory history
INNER JOIN sysjobs job
ON job.job_id = history.job_id
LEFT OUTER JOIN [dbo].[sysjobsteps] step
ON step.job_id = job.job_id
AND step.step_id = job.start_step_id
WHERE history.step_id = 0
AND CAST(dbo.agent_datetime(run_date, run_time) AS DATE) >= GETDATE() - 200
 

" 
    $result = Invoke-Sqlcmd -ErrorAction 'Stop' -ServerInstance $servername -query $sqlRec -database $collectDatabase
    foreach($line in $result){
                                      $insertReq =[String]::format(
                                      "insert into msdb.dbo.History_job
                                              (ServerName
                                              ,name
                                              ,run_time
                                              ,duration
                                              ,message
                                              ,run_status
                                              ,enabled
                                              ,run_date
                                              ,run_duration
                                              ,average_rt
                                              ,max_rt
                                              ,min_rt)
                                               values
                                           ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}')"
                                      
                                      ,@{$true="NULL";$false=($line.ServerName).ToString()}[$line.ServerName -eq [System.DBNull]::Value]
                                      ,@{$true="NULL";$false=($line.name).ToString()}[$line.name -eq [System.DBNull]::Value]
                                      ,@{$true="NULL";$false=($line.run_time).ToString()}[$line.run_time -eq [System.DBNull]::Value]
                                      ,@{$true="NULL";$false=($line.duration).ToString()}[$line.duration -eq [System.DBNull]::Value]
                                      ,@{$true="NULL";$false=($line.message).ToString()}[$line.message -eq [System.DBNull]::Value]  
                                      ,@{$true="NULL";$false=($line.run_status).ToString()}[$line.run_status -eq [System.DBNull]::Value]  
                                      ,@{$true="NULL";$false=($line.enabled).ToString() }[$line.enabled  -eq [System.DBNull]::Value]  
                                      ,@{$true="NULL";$false=($line.run_date).ToString() }[$line.run_date  -eq [System.DBNull]::Value]  
                                      ,@{$true="NULL";$false=($line.run_duration).ToString() }[$line.run_duration  -eq [System.DBNull]::Value]  
                                      ,@{$true="NULL";$false=($line.average_rt).ToString() }[$line.average_rt  -eq [System.DBNull]::Value]  
                                      ,@{$true="NULL";$false=($line.max_rt).ToString() }[$line.max_rt  -eq [System.DBNull]::Value]  
                                      ,@{$true="NULL";$false=($line.min_rt).ToString() }[$line.min_rt -eq [System.DBNull]::Value]  
                                       
                                  )
            $insertReq = $insertReq.Replace("'NULL'","NULL")
           
           $insertReq = $insertReq.Replace("l'étape","Etape")
            Invoke-Sqlcmd -ErrorAction 'Stop' -ServerInstance $CollectServer -Database $collectDatabase -query $insertReq
            }
        }
 