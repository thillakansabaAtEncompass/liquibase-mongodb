package liquibase.ext.mongodb.database;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import liquibase.database.ConnectionServiceFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.postgresql.Driver;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_METHOD)
class MongoConnectionTest {

    @Mock
    protected MongoClientDriver mongoDriverMock;

    @Mock
    protected Driver postgresDriverMock;

    @Mock
    protected MongoClient clientMock;

    @Mock
    protected MongoDatabase databaseMock;

    protected MongoConnection connection;

    @BeforeEach
    void setUp() {
        connection = new MongoConnection();
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void getPriority() {
        assertThat(connection.getPriority()).isEqualTo(501);
    }

    @SneakyThrows
    @Test
    void getAutoCommit() {
        assertThat(connection.getAutoCommit()).isFalse();
    }

    @SneakyThrows
    @Test
    void getDatabaseProductVersion() {
        assertThat(connection.getDatabaseProductVersion()).isEqualTo("0");
    }

    @SneakyThrows
    @Test
    void getDatabaseMajorVersion() {
        assertThat(connection.getDatabaseMajorVersion()).isEqualTo(0);
    }

    @SneakyThrows
    @Test
    void getDatabaseMinorVersion() {
        assertThat(connection.getDatabaseMinorVersion()).isEqualTo(0);
    }

    @SneakyThrows
    @Test
    void getConnectionUserName() {

        when(mongoDriverMock.connect(any(ConnectionString.class))).thenReturn(clientMock);
        when(clientMock.getDatabase(any())).thenReturn(databaseMock);

        assertThat(connection.getConnectionUserName()).isEmpty();

        connection.open("mongodb://localhost:27017/test_db", mongoDriverMock, null);
        assertThat(connection.getConnectionUserName()).isEmpty();

        connection.open("mongodb://user1:pass1@localhost:27017/test_db", mongoDriverMock, null);
        assertThat(connection.getConnectionUserName()).isEqualTo("user1");
    }

    @SneakyThrows
    @Test
    void isClosed() {

        when(mongoDriverMock.connect(any(ConnectionString.class))).thenReturn(clientMock);
        when(clientMock.getDatabase(any())).thenReturn(databaseMock);

        assertThat(connection.isClosed()).isTrue();

        connection.open("mongodb://localhost:27017/test_db?socketTimeoutMS=1000&connectTimeoutMS=1000&serverSelectionTimeoutMS=1000", mongoDriverMock, null);
        assertThat(connection.isClosed()).isFalse();

        connection.close();
        assertThat(connection.isClosed()).isTrue();

        //close a closed connection
        connection.close();
        assertThat(connection.isClosed()).isTrue();
    }

    @SneakyThrows
    @Test
    void getCatalog() {
        when(mongoDriverMock.connect(any(ConnectionString.class))).thenReturn(clientMock);
        when(clientMock.getDatabase(any())).thenReturn(databaseMock);
        when(databaseMock.getName()).thenReturn("test_db");
        when(databaseMock.withCodecRegistry(any())).thenReturn(databaseMock);

        assertThatExceptionOfType(DatabaseException.class).isThrownBy(() -> connection.getCatalog()).withCauseInstanceOf(NullPointerException.class);

        connection.open("mongodb://localhost:27017/test_db?socketTimeoutMS=1000&connectTimeoutMS=1000&serverSelectionTimeoutMS=1000", mongoDriverMock, null);
        assertThat(connection.getCatalog()).isEqualTo("test_db");
    }

    @SneakyThrows
    @Test
    void getDatabaseProductName() {
        assertThat(connection.getDatabaseProductName()).isEqualTo("MongoDB");
    }

    @SneakyThrows
    @Test
    void open() {
        when(mongoDriverMock.connect(any(ConnectionString.class))).thenReturn(clientMock);
        when(clientMock.getDatabase(any())).thenReturn(databaseMock);
        when(databaseMock.withCodecRegistry(any())).thenReturn(databaseMock);

        connection.open("mongodb://localhost:27017/test_db?socketTimeoutMS=1000&connectTimeoutMS=1000&serverSelectionTimeoutMS=1000", mongoDriverMock, null);
        assertThat(connection.getConnectionString().isSrvProtocol()).isFalse();
        assertThat(connection.getConnectionString().getDatabase()).isEqualTo("test_db");
        assertThat(connection.getConnectionString().getCredential()).isNull();
        assertThat(connection.getConnectionUserName()).isEmpty();
        assertThat(connection.getURL()).isEqualTo("localhost:27017");

        verify(mongoDriverMock).connect(any(ConnectionString.class));
        verify(clientMock).getDatabase(any());
        verify(databaseMock).withCodecRegistry(any());
        verifyNoMoreInteractions(mongoDriverMock, clientMock, databaseMock);

        connection.open("mongodb://user1:password1@localhost:27017/test_db?socketTimeoutMS=1000&connectTimeoutMS=1000&serverSelectionTimeoutMS=1000", mongoDriverMock, null);
        assertThat(connection.getConnectionString().isSrvProtocol()).isFalse();
        assertThat(connection.getConnectionString().getDatabase()).isEqualTo("test_db");
        assertThat(connection.getConnectionString().getCredential()).isNotNull();
        assertThat(connection.getConnectionString().getCredential().getUserName()).isEqualTo("user1");
        assertThat(connection.getConnectionString().getCredential().getPassword()).containsExactly('p', 'a', 's', 's', 'w', 'o', 'r', 'd', '1');
        assertThat(connection.getConnectionUserName()).isEqualTo("user1");
        assertThat(connection.getURL()).isEqualTo("localhost:27017");

        verify(mongoDriverMock, times(2)).connect(any(ConnectionString.class));
        verify(clientMock, times(2)).getDatabase(any());
        verify(databaseMock, times(2)).withCodecRegistry(any());
        verifyNoMoreInteractions(mongoDriverMock, clientMock, databaseMock);

        Properties properties = new Properties();
        properties.setProperty("user", "user2");
        properties.setProperty("password", "password2");
        connection.open("mongodb://mongodb1.example.com:27317,mongodb2.example.com:27017/test_db?socketTimeoutMS=1000&connectTimeoutMS=1000&serverSelectionTimeoutMS=1000", mongoDriverMock, properties);
        assertThat(connection.getConnectionString().isSrvProtocol()).isFalse();
        assertThat(connection.getConnectionString().getDatabase()).isEqualTo("test_db");
        assertThat(connection.getConnectionString().getCredential()).isNotNull();
        assertThat(connection.getConnectionString().getCredential().getUserName()).isEqualTo("user2");
        assertThat(connection.getConnectionString().getCredential().getPassword()).containsExactly('p', 'a', 's', 's', 'w', 'o', 'r', 'd', '2');
        assertThat(connection.getConnectionUserName()).isEqualTo("user2");
        assertThat(connection.getURL()).isEqualTo("mongodb1.example.com:27317,mongodb2.example.com:27017");

        verify(mongoDriverMock, times(3)).connect(any(ConnectionString.class));
        verify(clientMock, times(3)).getDatabase(any());
        verify(databaseMock, times(3)).withCodecRegistry(any());
        verifyNoMoreInteractions(mongoDriverMock, clientMock, databaseMock);

        properties = new Properties();
        properties.setProperty("user", "user3");
        connection.open("mongodb://localhost:27017/test_db?authMechanism=MONGODB-X509&socketTimeoutMS=1000&connectTimeoutMS=1000&serverSelectionTimeoutMS=1000", mongoDriverMock, properties);
        assertThat(connection.getConnectionString().isSrvProtocol()).isFalse();
        assertThat(connection.getConnectionString().getDatabase()).isEqualTo("test_db");
        assertThat(connection.getConnectionString().getCredential()).isNotNull();
        assertThat(connection.getConnectionString().getCredential().getUserName()).isEqualTo("user3");
        assertThat(connection.getConnectionString().getCredential().getPassword()).isNull();
        assertThat(connection.getConnectionUserName()).isEqualTo("user3");
        assertThat(connection.getURL()).isEqualTo("localhost:27017");

        verify(mongoDriverMock, times(4)).connect(any(ConnectionString.class));
        verify(clientMock, times(4)).getDatabase(any());
        verify(databaseMock, times(4)).withCodecRegistry(any());
        verifyNoMoreInteractions(mongoDriverMock, clientMock, databaseMock);
    }

    @SneakyThrows
    @Test
    @Disabled("Travis - DNS name not found")
    void openDNS() {
        when(mongoDriverMock.connect(any(ConnectionString.class))).thenReturn(clientMock);
        when(clientMock.getDatabase(any())).thenReturn(databaseMock);
        when(databaseMock.withCodecRegistry(any())).thenReturn(databaseMock);

        connection.open("mongodb+srv://localhost/test_db", mongoDriverMock, null);
        assertThat(connection.getConnectionString().isSrvProtocol()).isTrue();
        assertThat(connection.getConnectionString().getDatabase()).isEqualTo("test_db");
        assertThat(connection.getConnectionString().getCredential()).isNull();
        assertThat(connection.getConnectionUserName()).isEmpty();
        assertThat(connection.getURL()).isEqualTo("localhost");

        verify(mongoDriverMock).connect(any(ConnectionString.class));
        verify(clientMock).getDatabase(any());
        verify(databaseMock).withCodecRegistry(any());
        verifyNoMoreInteractions(mongoDriverMock, clientMock, databaseMock);

        connection.open("mongodb+srv://user1:password1@localhost/test_db", mongoDriverMock, null);
        assertThat(connection.getConnectionString().isSrvProtocol()).isTrue();
        assertThat(connection.getConnectionString().getDatabase()).isEqualTo("test_db");
        assertThat(connection.getConnectionString().getCredential()).isNotNull();
        assertThat(connection.getConnectionString().getCredential().getUserName()).isEqualTo("user1");
        assertThat(connection.getConnectionString().getCredential().getPassword()).containsExactly('p', 'a', 's', 's', 'w', 'o', 'r', 'd', '1');
        assertThat(connection.getConnectionUserName()).isEqualTo("user1");
        assertThat(connection.getURL()).isEqualTo("localhost");

        verify(mongoDriverMock, times(2)).connect(any(ConnectionString.class));
        verify(clientMock, times(2)).getDatabase(any());
        verify(databaseMock, times(2)).withCodecRegistry(any());
        verifyNoMoreInteractions(mongoDriverMock, clientMock, databaseMock);

        Properties properties = new Properties();
        properties.setProperty("user", "user2");
        properties.setProperty("password", "password2");
        connection.open("mongodb+srv://localhost/test_db", mongoDriverMock, properties);
        assertThat(connection.getConnectionString().isSrvProtocol()).isTrue();
        assertThat(connection.getConnectionString().getDatabase()).isEqualTo("test_db");
        assertThat(connection.getConnectionString().getCredential()).isNotNull();
        assertThat(connection.getConnectionString().getCredential().getUserName()).isEqualTo("user2");
        assertThat(connection.getConnectionString().getCredential().getPassword()).containsExactly('p', 'a', 's', 's', 'w', 'o', 'r', 'd', '2');
        assertThat(connection.getConnectionUserName()).isEqualTo("user2");
        assertThat(connection.getURL()).isEqualTo("localhost");

        verify(mongoDriverMock, times(3)).connect(any(ConnectionString.class));
        verify(clientMock, times(3)).getDatabase(any());
        verify(databaseMock, times(3)).withCodecRegistry(any());
        verifyNoMoreInteractions(mongoDriverMock, clientMock, databaseMock);

        properties = new Properties();
        properties.setProperty("user", "user3");
        connection.open("mongodb+srv://localhost/test_db?authMechanism=MONGODB-X509", mongoDriverMock, properties);
        assertThat(connection.getConnectionString().isSrvProtocol()).isTrue();
        assertThat(connection.getConnectionString().getDatabase()).isEqualTo("test_db");
        assertThat(connection.getConnectionString().getCredential()).isNotNull();
        assertThat(connection.getConnectionString().getCredential().getUserName()).isEqualTo("user3");
        assertThat(connection.getConnectionString().getCredential().getPassword()).isNull();
        assertThat(connection.getConnectionUserName()).isEqualTo("user3");
        assertThat(connection.getURL()).isEqualTo("localhost");

        verify(mongoDriverMock, times(4)).connect(any(ConnectionString.class));
        verify(clientMock, times(4)).getDatabase(any());
        verify(databaseMock, times(4)).withCodecRegistry(any());
        verifyNoMoreInteractions(mongoDriverMock, clientMock, databaseMock);
    }

    @Test
    void supportsDriver() {
        final MongoConnection mongoConnection = new MongoConnection();

        assertThat(mongoConnection.supports(null)).isFalse();
        assertThat(mongoConnection.supports(postgresDriverMock)).isFalse();
        assertThat(mongoConnection.supports(mongoDriverMock)).isTrue();
    }

    @Test
    void getDatabaseConnectionByDriver() {
        final ConnectionServiceFactory connectionServiceFactory = ConnectionServiceFactory.getInstance();

        assertThat(connectionServiceFactory.getDatabaseConnection(postgresDriverMock))
                .isInstanceOf(JdbcConnection.class);
        assertThat(connectionServiceFactory.getDatabaseConnection(mongoDriverMock))
                .isInstanceOf(MongoConnection.class);
    }

}