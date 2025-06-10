using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace Azunt.IncidentManagement
{
    public class IncidentsTableBuilder
    {
        private readonly string _masterConnectionString;
        private readonly ILogger<IncidentsTableBuilder> _logger;

        public IncidentsTableBuilder(string masterConnectionString, ILogger<IncidentsTableBuilder> logger)
        {
            _masterConnectionString = masterConnectionString;
            _logger = logger;
        }

        public void BuildTenantDatabases()
        {
            var tenantConnectionStrings = GetTenantConnectionStrings();

            foreach (var connStr in tenantConnectionStrings)
            {
                try
                {
                    EnsureIncidentsTable(connStr);
                    _logger.LogInformation($"Incidents table processed (tenant DB): {connStr}");
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"[{connStr}] Error processing tenant DB");
                }
            }
        }

        public void BuildMasterDatabase()
        {
            try
            {
                EnsureIncidentsTable(_masterConnectionString);
                _logger.LogInformation("Incidents table processed (master DB)");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing master DB");
            }
        }

        private List<string> GetTenantConnectionStrings()
        {
            var result = new List<string>();

            using (var connection = new SqlConnection(_masterConnectionString))
            {
                connection.Open();
                var cmd = new SqlCommand("SELECT ConnectionString FROM dbo.Tenants", connection);

                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var connectionString = reader["ConnectionString"]?.ToString();
                        if (!string.IsNullOrEmpty(connectionString))
                        {
                            result.Add(connectionString);
                        }
                    }
                }
            }

            return result;
        }

        private void EnsureIncidentsTable(string connectionString)
        {
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();

                var cmdCheck = new SqlCommand(@"
                    SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_NAME = 'Incidents'", connection);

                int tableCount = (int)cmdCheck.ExecuteScalar();

                if (tableCount == 0)
                {
                    var createSql = @"CREATE TABLE [dbo].[Incidents] (
    [ID] INT IDENTITY(1, 1) NOT NULL PRIMARY KEY,
    [ParentKey] NVARCHAR(255) NULL,
    [CaseNumber] NVARCHAR(250) NULL,
    [DailyLogID] INT NULL,
    [DailyLogNumber] NVARCHAR(MAX) NULL,
    [OpenedDate] SMALLDATETIME NULL,
    [Occurred] SMALLDATETIME NULL,
    [Closed] SMALLDATETIME NULL,
    [PropertyID] INT NULL,
    [Property] NVARCHAR(250) NULL,
    [ReportTypeId] INT NULL,
    [ReportSpecificId] INT NULL,
    [Specific] NVARCHAR(250) NULL,
    [SpecificID] INT NULL,
    [CaseStatusID] INT NULL,
    [LocationID] INT NULL,
    [Location] NVARCHAR(250) NULL,
    [SublocationID] INT NULL,
    [Sublocation] NVARCHAR(250) NULL,
    [DepartmentID] INT NULL,
    [Department] NVARCHAR(250) NULL,
    [Summary] NVARCHAR(MAX) NULL,
    [Details] NVARCHAR(MAX) NULL,
    [Resolution] NVARCHAR(MAX) NULL,
    [Reference] NVARCHAR(MAX) NULL,
    [SecondaryOperatorID] INT NULL,
    [SecondaryOperator] NVARCHAR(255) NULL,
    [SecondaryOperatorID2] INT NULL,
    [SecondaryOperator2] NVARCHAR(255) NULL,
    [FourthOperatorId] NVARCHAR(255) NULL,
    [FourthOperatorName] NVARCHAR(255) NULL,
    [Custodial] BIT NULL,
    [UseForce] BIT NULL,
    [Medical] BIT NULL,
    [RiskManagement] BIT NULL,
    [Active] BIT NULL,
    [Priority] NVARCHAR(255) NULL,
    [AgentName] NVARCHAR(MAX) NULL,
    [AgentSignature] NVARCHAR(MAX) NULL,
    [AgentTime] DATETIME NULL,
    [SupervisorName] NVARCHAR(MAX) NULL,
    [SupervisorSignature] NVARCHAR(MAX) NULL,
    [SupervisorTime] DATETIME NULL,
    [ManagerName] NVARCHAR(MAX) NULL,
    [ManagerSignature] NVARCHAR(MAX) NULL,
    [ManagerTime] DATETIME NULL,
    [DirectorName] NVARCHAR(MAX) NULL,
    [DirectorSignature] NVARCHAR(MAX) NULL,
    [DirectorTime] DATETIME NULL,
    [CaseTypeID] INT NULL,
    [InvestigatorID] INT NULL,
    [ShiftID] INT NULL,
    [ImmediateSupervisorID] INT NULL,
    [GamingClassID] INT NULL,
    [SurveillanceNotified] INT NULL,
    [SurveillanceObserverID] INT NULL,
    [InitialContact] NVARCHAR(250) NULL,
    [InspectorSig] NVARCHAR(250) NULL,
    [DeputyDirectorSig] NVARCHAR(250) NULL,
    [DirectorSig] NVARCHAR(250) NULL,
    [SupervisorSig] NVARCHAR(250) NULL,
    [CompletionDate] SMALLDATETIME NULL,
    [TGAForwardDate] SMALLDATETIME NULL,
    [TGOReturnDate] SMALLDATETIME NULL,
    [Citation] NVARCHAR(255) NULL,
    [ViolationNature] NVARCHAR(255) NULL,
    [Variance] MONEY NULL,
    [Employee] BIT NULL,
    [ManagerID] INT NULL,
    [TapeIdentification] NVARCHAR(250) NULL,
    [ActionTaken] NVARCHAR(250) NULL,
    [SuspectPhoto] NVARCHAR(250) NULL,
    [ExclusionInfo] NVARCHAR(255) NULL,
    [Notification] NVARCHAR(255) NULL,
    [PoliceContacted] BIT NULL,
    [PoliceContact] NVARCHAR(255) NULL,
    [InvestigatorSigTS] SMALLDATETIME NULL,
    [SupervisorSigTS] SMALLDATETIME NULL,
    [DeputyDirectorSigTS] SMALLDATETIME NULL,
    [DirectorSigTS] SMALLDATETIME NULL,
    [TGOResponse] NVARCHAR(255) NULL,
    [ClosedBy] NVARCHAR(250) NULL,
    [CreatedBy] NVARCHAR(255) NULL,
    [CreatedDate] DATETIMEOFFSET NULL,
    [ModifiedBy] NVARCHAR(255) NULL,
    [ModifiedDate] DATETIMEOFFSET NULL,
    [DispatchCallID] INT NULL,
    [AuditID] INT NULL,
    [SavingsOrLosses] BIT NULL,
    [DirectorOnly] BIT NULL,
    [CaseType] NVARCHAR(250) NULL,
    [Status] NVARCHAR(250) NULL,
    [Agent] NVARCHAR(250) NULL,
    [AgentSigFile] NVARCHAR(250) NULL,
    [SupervisorSigFile] NVARCHAR(250) NULL,
    [ManagerSigFile] NVARCHAR(250) NULL,
    [DirectorSigFile] NVARCHAR(250) NULL,
    [AgentImage] IMAGE NULL,
    [SupervisorImage] IMAGE NULL,
    [ManagerImage] IMAGE NULL,
    [DirectorImage] IMAGE NULL,
    [RemarksTitle1] NVARCHAR(MAX) NULL,
    [RemarksMemos1] NVARCHAR(MAX) NULL,
    [RemarksTitle2] NVARCHAR(MAX) NULL,
    [RemarksMemos2] NVARCHAR(MAX) NULL,
    [RemarksTitle3] NVARCHAR(MAX) NULL,
    [RemarksMemos3] NVARCHAR(MAX) NULL,
    [RemarksTitle4] NVARCHAR(MAX) NULL,
    [RemarksMemos4] NVARCHAR(MAX) NULL,
    [RemarksTitle5] NVARCHAR(MAX) NULL,
    [RemarksMemos5] NVARCHAR(MAX) NULL,
    [GeneratedReport] NVARCHAR(MAX) NULL,
    [Adjusted] BIT NULL,
    [IsArchive] BIT NULL,
    [SurveillanceName] NVARCHAR(250) NULL,
    [DivisionId] BIGINT NULL,
    [DivisionName] NVARCHAR(255) NULL
)";

                    var cmdCreate = new SqlCommand(createSql, connection);
                    cmdCreate.ExecuteNonQuery();
                    _logger.LogInformation("Incidents table created.");
                }
            }
        }

        public static void Run(IServiceProvider services, bool forMaster, string? optionalConnectionString = null)
        {
            try
            {
                var logger = services.GetRequiredService<ILogger<IncidentsTableBuilder>>();
                var config = services.GetRequiredService<IConfiguration>();

                string connectionString = !string.IsNullOrWhiteSpace(optionalConnectionString)
                    ? optionalConnectionString
                    : config.GetConnectionString("DefaultConnection") ?? throw new InvalidOperationException("DefaultConnection is not configured.");

                var builder = new IncidentsTableBuilder(connectionString, logger);

                if (forMaster)
                {
                    builder.BuildMasterDatabase();
                }
                else
                {
                    builder.BuildTenantDatabases();
                }
            }
            catch (Exception ex)
            {
                var fallbackLogger = services.GetService<ILogger<IncidentsTableBuilder>>();
                fallbackLogger?.LogError(ex, "Error while processing Incidents table.");
            }
        }
    }
}
