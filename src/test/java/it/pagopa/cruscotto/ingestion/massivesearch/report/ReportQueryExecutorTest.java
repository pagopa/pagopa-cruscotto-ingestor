package it.pagopa.cruscotto.ingestion.massivesearch.report;

import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.SimpleTransactionStatus;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifica il contratto di {@link ReportQueryExecutor}, non coperto dai test dei report: le sue
 * garanzie (cursore server-side e tetto di tempo lato server) si osservano solo contro un database
 * reale, quindi una rottura resterebbe invisibile fino alla produzione.
 */
class ReportQueryExecutorTest {

    private DataSource dataSource;
    private Connection connection;
    private Statement statement;
    private PreparedStatement preparedStatement;
    private PlatformTransactionManager transactionManager;
    private MassiveSearchProperties properties;

    @BeforeEach
    void setUp() throws SQLException {
        dataSource = mock(DataSource.class);
        connection = mock(Connection.class);
        statement = mock(Statement.class);
        preparedStatement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        transactionManager = mock(PlatformTransactionManager.class);
        properties = new MassiveSearchProperties();

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        // Percorso della query: un result set vuoto basta, qui interessa cio' che accade intorno.
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
    }

    private void stream() {
        new ReportQueryExecutor(dataSource, transactionManager, properties)
            .stream("SELECT 1", new MapSqlParameterSource(), rs -> { });
    }

    private String capturedSessionSql() throws SQLException {
        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(statement).execute(sql.capture());
        return sql.getValue();
    }

    /**
     * Il tetto va espresso in millisecondi: {@code toSeconds()} troncherebbe a 0 ogni durata sotto
     * il secondo e per PostgreSQL 0 significa "nessun limite", l'opposto dell'intento.
     */
    @Test
    void appliesStatementTimeoutInMilliseconds() throws SQLException {
        properties.getExecution().setStatementTimeout(Duration.ofMinutes(5));

        stream();

        assertThat(capturedSessionSql()).isEqualTo("SET LOCAL statement_timeout = 300000");
    }

    @Test
    void subSecondTimeoutIsNotTruncatedToNoLimit() throws SQLException {
        properties.getExecution().setStatementTimeout(Duration.ofMillis(250));

        stream();

        assertThat(capturedSessionSql()).isEqualTo("SET LOCAL statement_timeout = 250");
    }

    @Test
    void zeroTimeoutLeavesTheServerDefaultUntouched() throws SQLException {
        properties.getExecution().setStatementTimeout(Duration.ZERO);

        stream();

        verify(statement, never()).execute(anyString());
    }

    @Test
    void nullTimeoutLeavesTheServerDefaultUntouched() throws SQLException {
        properties.getExecution().setStatementTimeout(null);

        stream();

        verify(statement, never()).execute(anyString());
    }

    /**
     * Il timeout e' una rete di sicurezza: se il {@code SET LOCAL} non passa, il report deve
     * proseguire comunque invece di cadere.
     */
    @Test
    void reportProceedsWhenTheTimeoutCannotBeApplied() throws SQLException {
        when(statement.execute(anyString())).thenThrow(new SQLException("permission denied"));

        stream();

        verify(connection).prepareStatement(anyString());
    }

    /**
     * Il cursore server-side richiede un fetchSize positivo: con 0 PgJDBC materializza in memoria
     * l'intero result set, cioe' il problema che questa classe esiste per evitare.
     */
    @Test
    void appliesTheConfiguredFetchSizeToTheStatement() throws SQLException {
        properties.getExecution().setFetchSize(250);

        stream();

        verify(preparedStatement).setFetchSize(250);
    }

    @Test
    void nonPositiveFetchSizeIsClampedInsteadOfDisablingStreaming() throws SQLException {
        properties.getExecution().setFetchSize(0);

        stream();

        verify(preparedStatement).setFetchSize(1);
    }

    /**
     * Read-only: il percorso dei report non deve poter scrivere. REQUIRES_NEW: il confine e' la
     * singola query, senza ereditare transazioni altrui.
     */
    @Test
    void runsInAReadOnlyRequiresNewTransaction() {
        stream();

        ArgumentCaptor<TransactionDefinition> definition = ArgumentCaptor.forClass(TransactionDefinition.class);
        verify(transactionManager).getTransaction(definition.capture());
        assertThat(definition.getValue().isReadOnly()).isTrue();
        assertThat(definition.getValue().getPropagationBehavior())
            .isEqualTo(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    @Test
    void transactionIsCommittedOnSuccess() {
        stream();

        verify(transactionManager).commit(any(TransactionStatus.class));
    }
}
