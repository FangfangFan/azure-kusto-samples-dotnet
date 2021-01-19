using System;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;

namespace HelloKusto
{
    // This sample illustrates how to query Kusto using the Kusto.Data .NET library.
    //
    // For the purpose of demonstration, the query being sent retrieves multiple result sets.
    //
    // The program should execute in an interactive context (so that on first run the user
    // will get asked to sign in to Azure AD to access the Kusto service).
    class Program
    {
        //const string Cluster = "https://help.kusto.windows.net";
        //const string Database = "Samples";

        const string Cluster = "https://factordataexplorer.westus.kusto.windows.net";
        const string Database = "Factor";

        static void Main()
        {
            FindDependFactMissingTimeSlice();
        }

        static void FindDependFactMissingTimeSlice()
        {
            // The query provider is the main interface to use when querying Kusto.
            // It is recommended that the provider be created once for a specific target database,
            // and then be reused many times (potentially across threads) until it is disposed-of.
            var kcsb = new KustoConnectionStringBuilder(Cluster, Database)
                .WithAadUserPromptAuthentication();
            using (var queryProvider = KustoClientFactory.CreateCslQueryProvider(kcsb))
            {
                // The query -- Note that for demonstration purposes, we send a query that asks for two different
                var query =
                    @"let factstatus =
                    FactResults | summarize Records = count(), ProcessDate = max(ProcessDate) by FactDefinition_Id
                    | project FactDefinition_Id, ProcessDate
                    //// this condition might need to be changed. 
                    | where ProcessDate > ago(70d)
                    | extend DataAgeInDays = datetime_diff('day',now(),ProcessDate);
                    //// get LastException
                    let schedule =
                    cluster('factordataexplorer.westus.kusto.windows.net').database('AdHoc').get_latest_scheduler_info()
                    | project tostring(FactDefinitionId),LastUpdatedTime, LastException, CreatedTime
                    | join hint.strategy = shuffle factstatus
                    on $left.FactDefinitionId == $right.FactDefinition_Id
                    | project FactDefinitionId, ProcessDate, DataAgeInDays, LastUpdatedTime,LastException,CreatedTime;
                    //// get Name, Owner
                    let failedFactStatus=get_fact_definitions
                    | where Disabled == false
                    | project FactDefinition_Id, Name, Owner
                    | join hint.strategy = shuffle schedule
                    on $left.FactDefinition_Id == $right.FactDefinitionId
                    | where DataAgeInDays >= 0
                    | project-away FactDefinitionId;
                    //// get Fact Dependencies
                    let clusterName = 'https://ade.applicationinsights.io/subscriptions/42b49712-afaf-406b-a9c7-8c4325770324/resourcegroups/FactorResourcePlatform/providers/microsoft.insights/components/FactorAppInsightsProdPlatform';
                    let databaseName = 'FactorAppInsightsProdPlatform';
                    let factDependencies=
                    cluster(clusterName).database(databaseName).traces
                    | where operation_Name == 'CheckFactReadinessActivity'
                    | where timestamp >= ago(1d)
                    | where message startswith 'Fact:'
                    | parse message with 'Fact: ' FactDefinitionId ' - TimeSlice: ' TimeSlice ' - ' * 'Dependency: ' Dependency' - Offset: ' Offset ' - Ready: ' Satisfied
                      | summarize arg_max(todatetime(TimeSlice), *) by FactDefinitionId, Dependency, Satisfied
                    | project FactDefinitionId, Dependency, TimeSlice, Satisfied
                    | where isnotempty(Dependency) and Satisfied == 'False';
                    //// execute .show commands-and-queries to get FailureReason value.
                    let detailFailureReason=
                    evaluate execute_show_command(""https://factordataexplorer.westus.kusto.windows.net/factor"","".show commands-and-queries | where State == 'Failed' and Application == 'QuerySchedulerController' | parse-where Text with * \""_FactDefinition_Id='\"" FactDefinitionId \""'\"" * | summarize arg_max(StartedOn,FailureReason) by FactDefinitionId | project FactDefinitionId,StartedOn,FailureReason"");
                    let failedFactStatusSummary=
                    failedFactStatus
                    | join kind=leftouter hint.strategy = shuffle hint.remote=left factDependencies on $left.FactDefinition_Id==$right.FactDefinitionId
                    //// include NotSatisfiedDependency and NotSatisfiedTimeSlice only if LastException is 'Dependencies are not ready yet'
                    | extend NotSatisfiedDependency =iff(LastException contains_cs 'Dependencies are not ready yet', Dependency, '')
                    | extend NotSatisfiedTimeSlice =iff(LastException contains_cs 'Dependencies are not ready yet', TimeSlice, datetime(null))
                    | project FactDefinition_Id, FactName=Name, Owner, DataAgeInDays, LastException, NotSatisfiedDependency, NotSatisfiedTimeSlice, CreatedTime
                    | join kind=leftouter detailFailureReason on $left.FactDefinition_Id == $right.FactDefinitionId
                    //// include FailureReason only if LastException was truncated
                    | extend DetailedFailureReason = iff(LastException contains_cs'truncated', FailureReason, '')
                    | project-away FactDefinitionId, CreatedTime, StartedOn, FailureReason;
                    //// exception/solution table to lookup
                    let solution = datatable(exception:string,Solution:string)
                    [
                    'Low memory condition','Query has performance issue. Try to apply shuffle query strategy. Please refer to https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/shufflequery.',
                    'operator has exceeded the memory budget during evaluation','Query exceeds the Kusto default query limits. Please update the query. Refer to http://aka.ms/kustoquerylimits.',
                    'Failed to resolve scalar expression','Query has semantic error. Please update the query.',
                    'Dependencies are not ready yet','Please check the NotSatisfiedDependency and NotSatisfiedTimeSlice columns and trigger BackfillFact if necessary.'
                    ];
                    //// known issue table
                    let knownIssue = datatable(factId:string, factName:string, issue:string, action:string, workItem:string)
                    [
                    'da281320-cee8-46e0-a6e8-96860a8a9245','CAE Insights: Base','The data size this Fact produced exceeds the Kusto limits.','Owner is working on a solution.','',
                    '415e3c21-f502-43df-8097-aea0bbe020e9','Policy Insights: CAE Tenant','Depends on da281320-cee8-46e0-a6e8-96860a8a9245','Owner is working on a solution.','',
                    'b59d042b-5aa6-40a1-a9b6-c4aeb867128a','Policy Insights: CAE Dim Tenant Daily','Depends on da281320-cee8-46e0-a6e8-96860a8a9245','Owner is working on a solution.','',
                    '6a45fbe6-760d-4e2b-b5e5-6ed418216099','Policy Health Usage, Version and Validation: Reporting - CAE','Depends on da281320-cee8-46e0-a6e8-96860a8a9245','Owner is working on a solution.','',
                    '2d92a7fd-c1ba-4877-babe-f8b2a4bcbe80','Policy Insights: CAE detail daily','Depends on da281320-cee8-46e0-a6e8-96860a8a9245','Owner is working on a solution.','',
                    '7e4aa7b0-b28c-4a1e-8355-981404ccb4d2','DeviceCa DataInsights Usage Analyze','The data size this Fact produced exceeds the Kusto limits.','Owner is working on a solution.','',
                    '5340c959-3a88-4e18-9658-8eda606d122c','Device CA DataInsights Usage Report','Depends on 7e4aa7b0-b28c-4a1e-8355-981404ccb4d2','Owner is working on a solution.','',
                    '4a20f311-aeca-452d-9163-7d8667dd025d','Tenant Meta Data'
                    ,'Depends on 7e4aa7b0-b28c-4a1e-8355-981404ccb4d2','Owner is working on a solution.',''
                    ];
                    //// cross join to find the Fact which meet the exception/solution
                    let resolution =
                    failedFactStatusSummary
                    | extend dummy=1 | join kind=inner (solution | extend dummy=1) on dummy
                    | where LastException contains_cs exception or DetailedFailureReason contains_cs exception
                    | project FactDefinition_Id,Solution;
                    failedFactStatusSummary
                    | join kind=leftouter resolution on FactDefinition_Id
                    | project-away FactDefinition_Id1
                    //// exclude the known issue
                    //| join kind=leftanti knownIssue on $left.FactDefinition_Id==$right.factId
                    | where isnotempty(NotSatisfiedDependency) 
                    | order by DataAgeInDays
                    | extend NotSatisfiedDependFactId= extract(""([a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12})"",1,NotSatisfiedDependency)
                    | project FactDefinition_Id,FactName,Owner,DataAgeInDays,NotSatisfiedDependFactId,NotSatisfiedTimeSlice,NotSatisfiedDependency
                    | where DataAgeInDays >= 1 
                    | where DataAgeInDays <= 5;
                     ";

                // It is strongly recommended that each request has its own unique
                // request identifier. This is mandatory for some scenarios (such as cancelling queries)
                // and will make troubleshooting easier in others.
                var clientRequestProperties = new ClientRequestProperties() { ClientRequestId = Guid.NewGuid().ToString() };
                using (var reader = queryProvider.ExecuteQuery(query, clientRequestProperties))
                {
                    #region FFF
                    // Read Records
                    while (reader.Read())
                    {
                        if (reader.GetValue(4).ToString() != "")
                        {
                            query = @"let get_DependencyFact_Missing_TimeSlice=(factDefinitionId:string,timeSlice:datetime){
                            let step = toscalar(
                            get_measure_time_slices(factDefinitionId)
                            | where TimeSlice between((timeSlice - 1d)..(timeSlice - 1tick))
                            | summarize count() > 1);
                            get_measure_time_slices(factDefinitionId)
                            | make-series counter = count() default = 0
                            on TimeSlice in range(timeSlice, (timeSlice + 1d - 1tick), iff(step, 1h, 1d))
                            | project  counter, TimeSlice
                            | mvexpand  counter,TimeSlice
                            | where counter == 0
                            | project TimeSlice
                            }; get_DependencyFact_Missing_TimeSlice('" + reader.GetValue(4).ToString() + "', datetime(" + reader.GetValue(5).ToString() + ")); ";
                            Console.WriteLine("\nThe Missing Time Slice: '{0}' for FactID: '{1}'\t", reader.GetValue(5).ToString(), reader.GetValue(0).ToString());
                            Console.WriteLine("The Fact Missing Time Slices for DependFactID: '{0}'", reader.GetValue(4).ToString());
                            using (var dependFactReader = queryProvider.ExecuteQuery(query, clientRequestProperties))
                            {
                                while (dependFactReader.Read())
                                {
                                    if (dependFactReader.GetValue(0).ToString() != "")
                                    {
                                        Console.WriteLine("{0}\t", dependFactReader.GetValue(0));
                                    }
                                }
                            }
                        }
                    }
                    #endregion FFF
                }
            }
        }

        static void TestKustoQuery()
        {
            // The query provider is the main interface to use when querying Kusto.
            // It is recommended that the provider be created once for a specific target database,
            // and then be reused many times (potentially across threads) until it is disposed-of.
            var kcsb = new KustoConnectionStringBuilder(Cluster, Database)
                .WithAadUserPromptAuthentication();
            using (var queryProvider = KustoClientFactory.CreateCslQueryProvider(kcsb))
            {
                // The query -- Note that for demonstration purposes, we send a query that asks for two different
                // result sets (HowManyRecords and SampleRecords).
                var factId = "'dd2942c5-d40c-498d-bfcb-d490b78635ef'";
                var query = "get_measure_ingestion_status(" + factId + ",5d)";

                // It is strongly recommended that each request has its own unique
                // request identifier. This is mandatory for some scenarios (such as cancelling queries)
                // and will make troubleshooting easier in others.
                var clientRequestProperties = new ClientRequestProperties() { ClientRequestId = Guid.NewGuid().ToString() };
                using (var reader = queryProvider.ExecuteQuery(query, clientRequestProperties))
                {
                    #region Orignal Code
                    /*
                    // Read HowManyRecords
                    while (reader.Read())
                    {
                        var howManyRecords = reader.GetInt64(0);
                        Console.WriteLine($"There are {howManyRecords} records in the table");
                    }

                    // Move on to the next result set, SampleRecords
                    reader.NextResult();
                    Console.WriteLine();
                    while (reader.Read())
                    {
                        // Important note: For demonstration purposes we show how to read the data
                        // using the "bare bones" IDataReader interface. In a production environment
                        // one would normally use some ORM library to automatically map the data from
                        // IDataReader into a strongly-typed record type (e.g. Dapper.Net, AutoMapper, etc.)
                        DateTime time = reader.GetDateTime(0);
                        string type = reader.GetString(1);
                        string state = reader.GetString(2);
                        Console.WriteLine("{0}\t{1,-20}\t{2}", time, type, state);
                    }
                    */
                    #endregion Orignal Code

                    #region FFF
                    // Read Records
                    while (reader.Read())
                    {
                        Console.WriteLine("{0}\t{1}\t{2}\t{3}", reader.GetValue(0), reader.GetValue(1), reader.GetValue(2), reader.GetValue(3));
                        factId = reader.GetValue(0).ToString();
                        query = "get_measure_ingestion_status('" + factId + "',5d)";
                        using (var testReader = queryProvider.ExecuteQuery(query, clientRequestProperties))
                        {
                            while (testReader.Read())
                            {
                                Console.WriteLine("{0}\t{1}\t{2}\t{3}", testReader.GetValue(0), testReader.GetValue(1), testReader.GetValue(2), testReader.GetValue(3));
                            }
                        }
                    }
                    #endregion FFF
                }
            }
        }
    }

}
