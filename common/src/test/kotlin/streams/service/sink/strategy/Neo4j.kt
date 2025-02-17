package streams.service.sink.strategy

import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.ArgumentsProvider
import org.neo4j.caniuse.Neo4j
import org.neo4j.caniuse.Neo4jDeploymentType
import org.neo4j.caniuse.Neo4jEdition
import org.neo4j.caniuse.Neo4jVersion
import java.util.stream.Stream

internal val Neo4jV4Aura = Neo4j(Neo4jVersion(4, 4, 0), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.AURA)
internal val Neo4jV4OnPrem = Neo4j(Neo4jVersion(4, 4, 41), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.SELF_MANAGED)
internal val Neo4jV4Community = Neo4j(Neo4jVersion(4, 4, 41), Neo4jEdition.COMMUNITY, Neo4jDeploymentType.SELF_MANAGED)
internal val Neo4jV519Aura = Neo4j(Neo4jVersion(5, 19, 0), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.AURA)
internal val Neo4jV519OnPrem = Neo4j(Neo4jVersion(5, 19, 0), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.SELF_MANAGED)
internal val Neo4jV519Community =
    Neo4j(Neo4jVersion(5, 19, 0), Neo4jEdition.COMMUNITY, Neo4jDeploymentType.SELF_MANAGED)
internal val Neo4jV5LTSAura = Neo4j(Neo4jVersion(5, 26, 1), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.AURA)
internal val Neo4jV5LTSOnPrem = Neo4j(Neo4jVersion(5, 26, 1), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.SELF_MANAGED)
internal val Neo4jV5LTSCommunity =
    Neo4j(Neo4jVersion(5, 26, 1), Neo4jEdition.COMMUNITY, Neo4jDeploymentType.SELF_MANAGED)
internal val Neo4jV2025Aura = Neo4j(Neo4jVersion(2025, 1, 0), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.AURA)
internal val Neo4jV2025OnPrem =
    Neo4j(Neo4jVersion(2025, 1, 0), Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.SELF_MANAGED)
internal val Neo4jV2025Community =
    Neo4j(Neo4jVersion(2025, 1, 0), Neo4jEdition.COMMUNITY, Neo4jDeploymentType.SELF_MANAGED)
internal val Neo4jVLatestAura = Neo4j(Neo4jVersion.LATEST, Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.AURA)
internal val Neo4jVLatestOnPrem =
    Neo4j(Neo4jVersion.LATEST, Neo4jEdition.ENTERPRISE, Neo4jDeploymentType.SELF_MANAGED)
internal val Neo4jVLatestCommunity =
    Neo4j(Neo4jVersion.LATEST, Neo4jEdition.COMMUNITY, Neo4jDeploymentType.SELF_MANAGED)

class SupportedVersionsProvider : ArgumentsProvider {
    override fun provideArguments(context: ExtensionContext?): Stream<out Arguments?>? {
        return Stream.of(
            Arguments.of(Neo4jV4Aura, ""),
            Arguments.of(Neo4jV4OnPrem, ""),
            Arguments.of(Neo4jV4Community, ""),
            Arguments.of(Neo4jV519Aura, ""),
            Arguments.of(Neo4jV519OnPrem, ""),
            Arguments.of(Neo4jV519Community, ""),
            Arguments.of(Neo4jV5LTSAura, "CYPHER 5 "),
            Arguments.of(Neo4jV5LTSOnPrem, "CYPHER 5 "),
            Arguments.of(Neo4jV5LTSCommunity, "CYPHER 5 "),
            Arguments.of(Neo4jV2025Aura, "CYPHER 5 "),
            Arguments.of(Neo4jV2025OnPrem, "CYPHER 5 "),
            Arguments.of(Neo4jV2025Community, "CYPHER 5 "),
            Arguments.of(Neo4jVLatestAura, "CYPHER 5 "),
            Arguments.of(Neo4jVLatestOnPrem, "CYPHER 5 "),
            Arguments.of(Neo4jVLatestCommunity, "CYPHER 5 ")
        )
    }

}
