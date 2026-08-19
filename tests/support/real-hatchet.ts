import { PostgreSqlContainer } from "@testcontainers/postgresql";
import { ConfigProvider, Effect, Layer } from "effect";
import { GenericContainer, Network, Wait } from "testcontainers";
import { Hatchet } from "../../src/index.js";

/** Hard-coded default tenant seeded by the hatchet-lite quickstart on first boot. */
const SEEDED_TENANT_ID = "707d0855-80ab-4e1f-a156-f1c4546cbf52";

const POSTGRES_ALIAS = "hatchet-postgres";
const API_PORT = 8888;
const GRPC_PORT = 7077;
const HEALTHCHECK_PORT = 8733;

/**
 * Boots postgres + hatchet-lite in a shared Docker network, mints a tenant
 * token, and returns the connection info `Hatchet.layer` needs. Torn down in
 * reverse order (hatchet-lite → postgres → network) when the enclosing scope
 * closes.
 */
const acquireConnection = Effect.gen(function* () {
	const network = yield* Effect.acquireRelease(
		Effect.promise(() => new Network().start()),
		(network) => Effect.promise(() => network.stop()),
	);

	const postgres = yield* Effect.acquireRelease(
		Effect.promise(() =>
			new PostgreSqlContainer("postgres:16-alpine")
				.withNetwork(network)
				.withNetworkAliases(POSTGRES_ALIAS)
				.start(),
		),
		(container) => Effect.promise(() => container.stop()),
	);

	const databaseUrl = `postgresql://${postgres.getUsername()}:${postgres.getPassword()}@${POSTGRES_ALIAS}:5432/${postgres.getDatabase()}?sslmode=disable`;

	const hatchetLite = yield* Effect.acquireRelease(
		Effect.promise(() =>
			new GenericContainer("ghcr.io/hatchet-dev/hatchet/hatchet-lite:latest")
				.withNetwork(network)
				.withExposedPorts(API_PORT, GRPC_PORT, HEALTHCHECK_PORT)
				.withEnvironment({
					DATABASE_URL: databaseUrl,
					SERVER_AUTH_COOKIE_DOMAIN: "localhost",
					SERVER_AUTH_COOKIE_INSECURE: "t",
					SERVER_GRPC_BIND_ADDRESS: "0.0.0.0",
					SERVER_GRPC_INSECURE: "t",
					SERVER_GRPC_PORT: String(GRPC_PORT),
					SERVER_GRPC_BROADCAST_ADDRESS: `localhost:${GRPC_PORT}`,
					SERVER_URL: `http://localhost:${API_PORT}`,
					SERVER_AUTH_SET_EMAIL_VERIFIED: "t",
				})
				.withWaitStrategy(
					Wait.forHttp("/ready", HEALTHCHECK_PORT).forStatusCode(200),
				)
				// Hatchet's own tooling budgets ~2 minutes for migrations on first boot.
				.withStartupTimeout(180_000)
				.start(),
		),
		(container) => Effect.promise(() => container.stop()),
	);

	const token = yield* Effect.promise(async () => {
		const result = await hatchetLite.exec([
			"/hatchet-admin",
			"token",
			"create",
			"--config",
			"/config",
			"--tenant-id",
			SEEDED_TENANT_ID,
		]);
		if (result.exitCode !== 0) {
			throw new Error(
				`hatchet-admin token create failed (exit ${result.exitCode}): ${result.stderr}`,
			);
		}
		const trimmed = result.stdout.trim();
		if (trimmed.length === 0) {
			throw new Error(
				`hatchet-admin token create produced an empty token. stderr: ${result.stderr}`,
			);
		}
		return trimmed;
	});

	return {
		token,
		hostPort: `${hatchetLite.getHost()}:${hatchetLite.getMappedPort(GRPC_PORT)}`,
		apiUrl: `http://${hatchetLite.getHost()}:${hatchetLite.getMappedPort(API_PORT)}`,
	};
});

/**
 * A `Layer<Hatchet>` backed by a real hatchet-lite server running in Docker
 * via testcontainers. Requires a reachable Docker daemon; boots postgres and
 * hatchet-lite in a scoped resource, so the containers are torn down when the
 * layer's scope closes.
 */
export const layerRealHatchet: Layer.Layer<Hatchet> = Layer.unwrapScoped(
	Effect.gen(function* () {
		const connection = yield* acquireConnection;
		const configProvider = ConfigProvider.fromMap(
			new Map([
				["HATCHET_CLIENT_TOKEN", connection.token],
				["HATCHET_CLIENT_HOST_PORT", connection.hostPort],
				["HATCHET_CLIENT_API_URL", connection.apiUrl],
				["HATCHET_CLIENT_TLS_STRATEGY", "none"],
			]),
		);

		// The config values above are all supplied by us and guaranteed
		// well-formed (a hard-coded literal, or fields validated non-empty
		// right after minting them) — a ConfigError here would mean this
		// module has a bug, not something a caller can recover from.
		return Hatchet.layer({ runPrefersThisWorker: true }).pipe(
			Layer.provide(Layer.setConfigProvider(configProvider)),
			Layer.orDie,
		);
	}),
);
