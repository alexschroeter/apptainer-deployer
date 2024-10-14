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

from fakts import get_current_fakts
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
    endpoint_url: str = field(default="arkitekt.compeng.uni-frankfurt.de")


@startup
async def on_startup(instance_id) -> ArkitektContext:
    print("Starting up on_startup", instance_id)
    print("Check sfosr scontainers that are no longer pods?")

    x = await adeclare_backend(instance_id=instance_id, name="Apptainer-Deployer", kind="apptainer")

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
            if not container["instance"].startswith("arkitekt-"):
                break
            try:
                old_status = pod_status.get(container["instance"], None) # I am not sure if this is the right way to do this
                print("Pod Status: ",old_status)
                print(f"Container Checker currently checking {container['instance']} of {[d['instance'] for d in containers['instances'] if 'instance' in d]}")
                if container['instance'] != old_status:
                    p = await aupdate_pod(
                        local_id=container["instance"],
                        status=PodStatus.RUNNING,
                        instance_id=context.instance_id,
                    )
                    print("Updated Container Status")
                    with open(f"apptainer-{container['instance']}.log", "r") as f:
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
    with open(f"apptainer-{pod.pod_id}.log", "r") as f:
        logs = f.read()
        await adump_logs(pod.id, logs)
    return pod


@register(name="Runner")
def run(deployment: Deployment, context: ArkitektContext, pod: Pod) -> Pod:
    print("\tRunner:\n",deployment)
    z = create_pod(
        deployment=deployment, instance_id=useInstanceID(), local_id=pod.pod_id
    )
    print(z)
    return z


@register(name="Restart")
def restart(pod: Pod, context: ArkitektContext) -> Pod:
    """Restart

    Restarts a pod by stopping and starting it again.

    """
    print(f"stopping container {container_name}")
    subprocess.run(["apptainer", "instance", "stop", pod.pod_id])
    print(f"(Re-)starting container {container_name}")
    subprocess.run(
        ["apptainer", "instance", "start", pod.deployment.flavour.image, pod.pod_id],
        text=True,
    )
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
    container_name = "arkitekt-"+str(uuid.uuid4())
    flavour = release.flavours[0]

    progress(0)

    # ToDo: How should this be handled see https://github.com/alexschroeter/apptainer-deployer/issues/2
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
        ["apptainer", "instance", "start", "--writable-tmpfs", f"docker://{flavour.image}", container_name],
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

    z = create_pod(
        deployment=deployment, instance_id=useInstanceID(), local_id=container_name
    )

    print("Running the command")
    with open(f"apptainer-{container_name}.log", "w") as f:
        process = subprocess.run(
            ["apptainer", "exec", "--pwd", "/app", "instance://"+container_name, "arkitekt-next", "run", "prod", "--url", f"{context.endpoint_url}"],
            stdout=f,
            )

    print(process)
    print(f"Deployed container with id {container_name} on network {network} with token {token} and target url {str(context.endpoint_url)}")

    progress(90)

    print("Pod Created during Deploy: ",z)
    return z


@register(name="Progresso")
def progresso():
    for i in range(10):
        print("Sending progress")
        progress(i * 10)
        time.sleep(1)

    return None
