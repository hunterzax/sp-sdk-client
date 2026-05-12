package com.ptt.client.config;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

import javax.sql.DataSource;

/**
 * Optional second datasource: Microsoft SQL Server mirror table.
 * Primary {@code spring.datasource} remains the local H2 dashboard store.
 *
 * <p>CWE-260: Credentials are read from the Spring {@link Environment} and passed
 * directly to the connection pool without storing them as {@code String} fields or
 * method parameters visible in stack traces, actuator {@code /beans}, or heap dumps.
 * The {@link Environment} is a short-lived parameter and the character data is held
 * only inside HikariCP's own pool configuration (which masks passwords in toString).
 */
@Configuration
public class MssqlWriterDataSourceConfig {

    @Bean(name = "primaryDataSource")
    @Primary
    public DataSource primaryDataSource(
            @Value("${client.localdb.url:jdbc:h2:file:./data/sensor_data}") String url,
            @Value("${client.localdb.driver-class-name:org.h2.Driver}") String driverClassName,
            Environment env) {
        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl(url);
        ds.setDriverClassName(driverClassName);
        // CWE-260: resolve credentials from Environment at configuration time only;
        // they are never stored as class-level String fields.
        ds.setUsername(env.getRequiredProperty("client.localdb.username"));
        ds.setPassword(env.getRequiredProperty("client.localdb.password"));
        ds.setPoolName("local-dashboard-pool");
        ds.setMaximumPoolSize(10);
        ds.setMinimumIdle(1);
        return ds;
    }

    @Bean(name = "localJdbcTemplate")
    @Primary
    public JdbcTemplate localJdbcTemplate(@Qualifier("primaryDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    @Bean(name = "mssqlWriterDataSource")
    @ConditionalOnProperty(name = "dbwriter.enabled", havingValue = "true")
    public DataSource mssqlWriterDataSource(
            @Value("${dbwriter.url}") String url,
            Environment env) {
        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl(url);
        // CWE-260: resolve credentials from Environment at configuration time only;
        // they are never stored as class-level String fields.
        ds.setUsername(env.getRequiredProperty("dbwriter.username"));
        ds.setPassword(env.getRequiredProperty("dbwriter.password"));
        ds.setDriverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        ds.setPoolName("mssql-writer-pool");
        ds.setMaximumPoolSize(10);
        ds.setMinimumIdle(1);
        return ds;
    }

    @Bean(name = "mssqlWriterJdbcTemplate")
    @ConditionalOnProperty(name = "dbwriter.enabled", havingValue = "true")
    public JdbcTemplate mssqlWriterJdbcTemplate(@Qualifier("mssqlWriterDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }
}
