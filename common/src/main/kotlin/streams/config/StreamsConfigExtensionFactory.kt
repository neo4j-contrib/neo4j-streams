package streams.config

import org.neo4j.configuration.Config
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.kernel.extension.ExtensionFactory
import org.neo4j.kernel.extension.ExtensionType
import org.neo4j.kernel.extension.context.ExtensionContext
import org.neo4j.kernel.lifecycle.Lifecycle
import org.neo4j.logging.internal.LogService

class StreamsConfigExtensionFactory: ExtensionFactory<StreamsConfigExtensionFactory.Dependencies>(ExtensionType.GLOBAL, StreamsConfig::class.java.simpleName) {
    interface Dependencies {
        fun log(): LogService
        fun config(): Config
    }

    override fun newInstance(context: ExtensionContext, dependencies: Dependencies): Lifecycle {
        return StreamsConfig(dependencies.config(), dependencies.log())
    }
}
