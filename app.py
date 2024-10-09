import asyncio
import datetime
import os
import time
from dataclasses import dataclass, field
from typing import AsyncGenerator, Dict
import xarray as xr
import numpy as np
# from docker import DockerClient, from_env
# import spython.main as SingularityClient
import subprocess, json, uuid

from arkitekt_next import background, easy, register, startup
from rekuest_next.agents.context import context
from kabinet.api.schema import (
    Backend,
    Deployment,
    Pod,
    PodStatus,
    Release,
    adeclare_backend,
    adump_logs,
    aupdate_pod,
    create_deployment,
    create_pod,
    delete_pod,
)
from mikro_next.api.schema import Image, from_array_like
from rekuest_next.actors.reactive.api import (
    progress,
    log,
    useInstanceID,
)
from unlok_next.api.schema import (
    DevelopmentClientInput,
    ManifestInput,
    Requirement,
    create_client,
)

# ToDo: For full functionality, we need:
# - an automatic unique name generator for the containers (currently hardcoded to random-name)
# - currently containers unknown by arkitekt are killed by the checker I believe
# - currently running a bit of a mixture between apptainer instance handling and subprocess
#   This should probably be made consistent. Problem is that the apptainer instance logging doesn't seem to work as expected.
#   Also using subprocess for the handling might make it easier to port to other container systems because the handling is already there.

ME = os.getenv("INSTANCE_ID", "FAKE GOD")
ARKITEKT_GATEWAY = os.getenv("ARKITEKT_GATEWAY", "caddy")
ARKITEKT_NETWORK = os.getenv("ARKITEKT_NETWORK", "next_default")


@context
@dataclass
class ArkitektContext:
    backend: Backend
    docker: None
    instance_id: str
    gateway: str = field(default=ARKITEKT_GATEWAY)
    network: str = field(default=ARKITEKT_NETWORK)


@startup
async def on_startup(instance_id) -> ArkitektContext:
    print("Starting up on_startup", instance_id)
    print("Check sfosr scontainers that are no longer pods?")

    x = await adeclare_backend(instance_id=instance_id, name="::", kind="apptainer")

    return ArkitektContext(
        docker=None,
        gateway=ARKITEKT_GATEWAY,
        network=ARKITEKT_NETWORK,
        backend=x,
        instance_id=instance_id,
    )


@background
async def container_checker(context: ArkitektContext):
    print("Starting up container_checker")
    print("Check for containers that are dno longer pods?")

    pod_status: Dict[str, PodStatus] = {}

    while True:
        apptainer_instance_list = subprocess.run(["apptainer", "instance", "list", "--json"], text=True, capture_output=True)
        containers = json.loads(apptainer_instance_list.stdout)
        for container in containers["instances"]:
            try:
                old_status = pod_status.get(container["instance"], None)
                print("Pod Status: ",old_status)
                print(f"Container Checker currently checking {container['instance']} of {[d['instance'] for d in containers['instances'] if 'instance' in d]}")
                if container['instance'] != old_status:
                    p = await aupdate_pod(
                        local_id=container["instance"],
                        status=PodStatus.RUNNING,
                        instance_id=context.instance_id,
                    )
                    print("Updated Container Status")

                    # with open(container["logOutPath"], "r") as f: # If apptainer instance logging is working
                    with open("apptainer.log", "r") as f:
                        logs = f.read()
                        await adump_logs(p.id, logs)
            except Exception as e:
                print("Error updating pod status", e)
                subprocess.run(["apptainer", "instance", "stop", container["instance"]])
        else:
            if containers == []: print("No containers to check")

        await asyncio.sleep(5)


@register(name="dump_logs")
async def dump_logs(context: ArkitektContext, pod: Pod) -> Pod:
    print(pod.pod_id)
    apptainer_instance_list = subprocess.run(["apptainer", "instance", "list", "--json"], text=True, capture_output=True)
    containers = json.loads(apptainer_instance_list.stdout)
    for container in containers["instances"]:
        if container["instance"] == pod.pod_id:
            # with open(containers['instance']['logOutPath'], "r") as f: # If apptainer instance logging is working
            with open("apptainer.log", "r") as f:
                logs = f.read()
    await adump_logs(pod.id, logs)

    return pod


@register(name="Runner")
def run(deployment: Deployment, context: ArkitektContext) -> Pod:
    print("\tRunner:\n",deployment)
    # container = context.docker.containers.run(
    #     deployment.local_id, detach=True, labels={"arkitekt.live.kabinet": ME}
    # )
    container_name = "random-name"
    process = subprocess.run(
        ["apptainer", "instance", "start", "--writable-tmpfs", "docker://jhnnsrs/renderer:0.0.1-vanilla", container_name],
        text=True,
    )

    test = useInstanceID()
    print(test)

    z = create_pod(
        deployment=deployment, instance_id=test, local_id=container_name
    )

    print(z)
    return z


# ToDo: Restarting a container is not supported by apptainer
# might be possible with a workaround
@register(name="Restart")
def restart(pod: Pod, context: ArkitektContext) -> Pod:
    """Restart

    Restarts a pod by stopping and starting it again.


    """

    print("\tRunning\n")
    container = context.docker.containers.get(pod.pod_id)

    progress(50)
    container.restart()
    progress(100)
    return pod


@register(name="Move")
def move(pod: Pod) -> Pod:
    """Move"""
    print("Moving node")

    progress(0)

    # Simulating moving a node
    for i in range(10):
        progress(i * 10)
        time.sleep(1)

    return pod


@register(name="Stop")
def stop(pod: Pod, context: ArkitektContext) -> Pod:
    """Stop

    Stops a pod by stopping and does not start it again.

    """
    subprocess.run(["apptainer", "instance", "stop", str(pod.pod_id)])
    return pod


@register(name="Removed")
def remove(pod: Pod, context: ArkitektContext) -> Pod:
    """Remove

    Remove a pod by stopping and removing it.

    """
    subprocess.run(["apptainer", "instance", "stop", str(pod.pod_id)])
    return pod


@register(name="Deploy")
def deploy(release: Release, context: ArkitektContext) -> Pod:
    print(release)
    # docker = context.docker
    caddy_url = context.gateway
    network = context.network
    container_name = str(uuid.uuid4())
    flavour = release.flavours[0]

    progress(0)

    print("OAINDFOAIWNDOAINWDOIANWd")

    print(
        [Requirement(key=key, **value) for key, value in flavour.requirements.items()]
    )

    token = create_client(
        DevelopmentClientInput(
            manifest=ManifestInput(
                identifier=release.app.identifier,
                version=release.version,
                scopes=flavour.manifest["scopes"],
            ),
            requirements=[
                Requirement(key=key, **value)
                for key, value in flavour.requirements.items()
            ],
        )
    )

    process = subprocess.run(
        ["apptainer", "instance", "start", f"docker://{flavour.image}", container_name],
        text=True,
    )

    progress(10)

    deployment = create_deployment(
        flavour=flavour,
        instance_id=useInstanceID(),
        local_id=flavour.image,
        last_pulled=datetime.datetime.now(),
    )

    progress(30)

    print("Arkitekt_Gateway Variable: ", os.getenv("ARKITEKT_GATEWAY"))

    # COnver step here for apptainer

    print("Running the command")
    with open(f"apptainer-{z.id}.log", "w") as f:
        process = subprocess.run(
            ["apptainer", "exec", "--pwd", "/app", "instance://"+container_name, "arkitekt-next", "run", "prod", "--url", "arkitekt.compeng.uni-frankfurt.de"],
            stdout=f,
            )

    print(process)
    print(f"Deployed container with id {container_name} on network {network} with token {token} and target url {caddy_url}")

    progress(90)

    context.docker = container_name

    z = create_pod(
        deployment=deployment, instance_id=useInstanceID(), local_id=container_name
    )

    print("Pod Created during Deploy: ",z)

    return z

@register(name="Progresso")
def progresso():
    for i in range(10):
        print("Sending progress")
        progress(i * 10)
        time.sleep(1)

    return None
