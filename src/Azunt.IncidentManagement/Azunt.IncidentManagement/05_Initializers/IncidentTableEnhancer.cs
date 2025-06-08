using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;

namespace Azunt.IncidentManagement
{
    /// <summary>
    /// Incidents 테이블의 스키마를 점진적으로 확장하는 유틸리티 클래스
    /// </summary>
    public class IncidentTableEnhancer
    {
        private readonly string _connectionString;
        private readonly ILogger<IncidentTableEnhancer> _logger;

        public IncidentTableEnhancer(string connectionString, ILogger<IncidentTableEnhancer> logger)
        {
            _connectionString = connectionString;
            _logger = logger;
        }

        /// <summary>
        /// Incidents 테이블에 필요한 컬럼을 점진적으로 추가
        /// </summary>
        public void EnhanceIncidentTable()
        {
            var columnsToEnsure = new Dictionary<string, string>
            {
                { "DivisionId", "BIGINT NULL" },
                { "DivisionName", "NVARCHAR(255) NULL" },
                { "FourthOperatorId", "NVARCHAR(255) NULL" },
                { "FourthOperatorName", "NVARCHAR(255) NULL" }
            };

            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();

                foreach (var (columnName, columnDefinition) in columnsToEnsure)
                {
                    if (!ColumnExists(connection, "Incidents", columnName))
                    {
                        var alterCommand = new SqlCommand($@"
                            ALTER TABLE [dbo].[Incidents]
                            ADD [{columnName}] {columnDefinition};", connection);

                        alterCommand.ExecuteNonQuery();
                        _logger.LogInformation($"Column '{columnName}' added to Incidents table.");
                    }
                }

                connection.Close();
            }
        }

        private bool ColumnExists(SqlConnection connection, string tableName, string columnName)
        {
            var checkCommand = new SqlCommand(@"
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = @TableName AND COLUMN_NAME = @ColumnName", connection);

            checkCommand.Parameters.AddWithValue("@TableName", tableName);
            checkCommand.Parameters.AddWithValue("@ColumnName", columnName);

            return (int)checkCommand.ExecuteScalar() > 0;
        }

        /// <summary>
        /// 서비스 프로바이더 기반으로 실행할 수 있는 편의 메서드
        /// </summary>
        public static void Run(IServiceProvider services, bool forMaster, string? optionalConnectionString = null)
        {
            try
            {
                var logger = services.GetRequiredService<ILogger<IncidentTableEnhancer>>();
                var config = services.GetRequiredService<IConfiguration>();

                string connectionString;

                if (!string.IsNullOrWhiteSpace(optionalConnectionString))
                {
                    connectionString = optionalConnectionString;
                }
                else
                {
                    var tempConnectionString = config.GetConnectionString("DefaultConnection");
                    if (string.IsNullOrEmpty(tempConnectionString))
                        throw new InvalidOperationException("DefaultConnection is not configured in appsettings.json.");

                    connectionString = tempConnectionString;
                }

                var enhancer = new IncidentTableEnhancer(connectionString, logger);
                enhancer.EnhanceIncidentTable();
            }
            catch (Exception ex)
            {
                var fallbackLogger = services.GetService<ILogger<IncidentTableEnhancer>>();
                fallbackLogger?.LogError(ex, "Error while enhancing Incidents table schema.");
            }
        }
    }
}
