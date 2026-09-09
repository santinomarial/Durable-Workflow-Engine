# Provision and rotate API keys

The HTTP control plane defaults to fail closed: starting the API without
valid authentication configuration fails at startup. Authentication may be
disabled only by explicitly setting `DWE_AUTH_MODE=disabled`, and that mode is
for an isolated local development environment only — see
[the getting-started tutorial](../tutorials/getting-started.md) for where
that's appropriate.

## Generate a key

```shell
uv run engine auth-key --key-id oncall-admin --role admin
```

The command prints the token once and a `configuration` value containing only
its SHA-256 digest. Put the token in a secret manager and give it to the
operator through an approved secret-sharing channel — never in chat, a ticket,
or source control.

## Configure the API to accept it

Set one or more comma-separated digest entries on the API service:

```shell
export DWE_AUTH_MODE=required
export DWE_API_KEYS='dashboard:viewer:DIGEST,oncall:admin:DIGEST'
```

Key IDs may contain letters, numbers, hyphens, and underscores. The configured
digest is safe to compare without retaining the bearer credential, although it
should still be kept with deployment configuration. The service compares
digests in constant time, so this configuration is not itself sensitive in the
way the raw token is. See [roles](../reference/api-and-roles.md) for what each
role can do.

## Rotate a key

1. Generate the new key as above.
2. Deploy the API with **both** the old and new digests in `DWE_API_KEYS`.
3. Move clients over to the new token.
4. Remove the old entry from `DWE_API_KEYS` and redeploy.

## Revoke a key immediately

Remove its digest from `DWE_API_KEYS` and roll the API replicas. Use the
immutable audit log's `actor_key_id` plus request IDs to scope what actions
were taken with the key before revocation — see
[respond to an incident](respond-to-an-incident.md).
