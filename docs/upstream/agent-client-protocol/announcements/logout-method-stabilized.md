> ## Documentation Index
> Fetch the complete documentation index at: https://agentclientprotocol.com/llms.txt
> Use this file to discover all available pages before exploring further.

# Logout Method is stabilized

> Announcement that the logout method is now part of the stable ACP protocol.

**Published:** May 21, 2026

The [Logout Method RFD](/rfds/logout-method) has moved to Completed and the `logout` method is stabilized.

When advertised via `agentCapabilities.auth.logout`, Clients can now ask Agents to end the current authenticated state and return the connection to a state where future authentication-gated requests require `authenticate` again.

For the protocol documentation, see [Logging Out](/protocol/v1/authentication#logging-out).
