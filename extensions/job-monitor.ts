import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

const MCP_RUNTIME_REGISTER_EVENT = "pi-mcp-adapter:runtime-register:v1";

type RuntimeRegistrationRequest = {
  version: 1;
  name: string;
  definition: {
    command: string;
    cwd: string;
  };
  result?:
    | { ok: true; registration: { dispose(): Promise<void> } }
    | { ok: false; error: Error };
};

export default function jobMonitorExtension(pi: ExtensionAPI): void {
  let registration: { dispose(): Promise<void> } | undefined;

  pi.on("session_start", () => {
    if (registration) return;

    const packageRoot = dirname(dirname(fileURLToPath(import.meta.url)));
    const request: RuntimeRegistrationRequest = {
      version: 1,
      name: "pi_job_monitor__job_monitor",
      definition: {
        command: join(packageRoot, "scripts", "launch_job_monitor_mcp"),
        cwd: packageRoot,
      },
    };

    pi.events.emit(MCP_RUNTIME_REGISTER_EVENT, request);
    const result = request.result;
    if (!result) return;
    if (!result.ok) throw result.error;
    registration = result.registration;
  });

  pi.on("session_shutdown", async () => {
    const current = registration;
    registration = undefined;
    await current?.dispose();
  });
}
