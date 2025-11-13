package org.neo4j.test.rule;

import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.connectioninfo.RoutingInfo;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.TransactionExceptionMapper;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * JUnit @Rule for configuring, creating and managing an ImpermanentGraphDatabase instance.
 */
@Deprecated
public class ImpermanentDbmsRule extends DbmsRule
{
    private final InternalLogProvider userLogProvider;
    private final InternalLogProvider internalLogProvider;

    public ImpermanentDbmsRule()
    {
        this( null );
    }

    public ImpermanentDbmsRule( InternalLogProvider logProvider )
    {
        this.userLogProvider = logProvider;
        this.internalLogProvider = logProvider;
    }

    @Override
    public ImpermanentDbmsRule startLazily()
    {
        return (ImpermanentDbmsRule) super.startLazily();
    }

    @Override
    protected TestDatabaseManagementServiceBuilder newFactory()
    {
        return maybeSetInternalLogProvider( maybeSetUserLogProvider( new TestDatabaseManagementServiceBuilder().impermanent() ) );
    }

    protected final TestDatabaseManagementServiceBuilder maybeSetUserLogProvider( TestDatabaseManagementServiceBuilder factory )
    {
        return ( userLogProvider == null ) ? factory : factory.setUserLogProvider( userLogProvider );
    }

    protected final TestDatabaseManagementServiceBuilder maybeSetInternalLogProvider( TestDatabaseManagementServiceBuilder factory )
    {
        return ( internalLogProvider == null ) ? factory : factory.setInternalLogProvider( internalLogProvider );
    }

    @Override
    public TopologyGraphDbmsModel.HostedOnMode mode() {
        return null;
    }

    @Override
    public InternalTransaction beginTransaction(KernelTransaction.Type type, LoginContext loginContext, ClientConnectionInfo clientConnectionInfo, RoutingInfo routingInfo, List<String> list, long l, TimeUnit timeUnit, Consumer<Status> consumer, TransactionExceptionMapper transactionExceptionMapper) {
        return null;
    }

    @Override
    public boolean isAvailable() {
        return false;
    }
}
