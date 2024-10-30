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
import subprocess, json, uuid, yaml

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

ARKITEKT_APPTAINER_CONFIG = yaml.safe_load(open("apptainer_config.yaml"))

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
        apptainer_instance_list = subprocess.run([ARKITEKT_APPTAINER_CONFIG["ARKITEKT_APPTAINER_BIN"], "instance", "list", "--json"], text=True, capture_output=True)
        containers = json.loads(apptainer_instance_list.stdout)
        for container in containers["instances"]:
            # if not container["instance"].startswith("arkitekt-"):
            #     break
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
                subprocess.run(_get_command(variant="instance_stop", context=context, pod=p))
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
    # print(f"stopping container {container_name}")
    # subprocess.run(_get_command(variant="instance_stop"))
    # print(f"(Re-)starting container {container_name}")
    # subprocess.run(
    #     [ARKITEKT_APPTAINER_CONFIG["ARKITEKT_APPTAINER_BIN"], "instance", "start", pod.deployment.flavour.image.image_string, pod.pod_id],
    #     env=dict(os.environ)| ARKITEKT_APPTAINER_CONFIG,
    #     text=True,
    # )
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
    subprocess.run(_get_command(variant="instance_stop", context=context, pod=pod))
    return pod


@register(name="Removed")
def remove(pod: Pod, context: ArkitektContext) -> Pod:
    """Remove

    Remove a pod by stopping and removing it.

    """
    subprocess.run(_get_command(variant="instance_stop", context=context, pod=pod))
    return pod


@register(name="Deploy")
def deploy(release: Release, context: ArkitektContext) -> Pod:
    print(release)
    # docker = context.docker
    caddy_url = context.gateway
    network = context.network
    container_name = "arkitekt-"+str(uuid.uuid4())
    print("\nFALVOUR FLAV\n\n",release.flavours, "\n\n\n")
    flavour = _get_flavour(release.flavours)

    progress(0)

    # ToDo: How should this be handled see https://github.com/alexschroeter/apptainer-deployer/issues/2
    print(
        [Requirement(**req.model_dump()) for req in flavour.requirements]
    )

    token = create_client(
        DevelopmentClientInput(
            manifest=ManifestInput(
                identifier=release.app.identifier,
                version=release.version,
                scopes=flavour.manifest["scopes"],
            ),
            requirements=[Requirement(**req.model_dump()) for req in flavour.requirements],
        )
    )

    print(_get_command(variant="start_instance", context=context, container_name=container_name, release=release))
    start_apptainer_instance = subprocess.run(
        _get_command(variant="start_instance", context=context, container_name=container_name, release=release),
        env=dict(os.environ)| ARKITEKT_APPTAINER_CONFIG,
        text=True,
    )
    print(f"started instance with id {container_name}")

    progress(10)

    deployment = create_deployment(
        flavour=flavour,
        instance_id=useInstanceID(),
        local_id=flavour.image.image_string,
        last_pulled=datetime.datetime.now(),
    )

    progress(30)

    print("Arkitekt_Gateway Variable: ", os.getenv("ARKITEKT_GATEWAY"))

    z = create_pod(
        deployment=deployment, instance_id=useInstanceID(), local_id=container_name
    )

    # print("#######", flavour.gpu_type)
    # apptainer_run_command = [ARKITEKT_APPTAINER_CONFIG["ARKITEKT_APPTAINER_BIN"], "exec", "--pwd", str(docker_inspect_workdir.stdout.replace("'","").replace("\n","")), "instance://"+container_name, "arkitekt-next", "run", "prod", "--url", f"{context.endpoint_url}"]
    # apptainer_run_command = apptainer_run_command[:4] + gpu_parameter("nvidia") + apptainer_run_command[4:]
    # # apptainer_run_command = ["env"]
    # print(apptainer_run_command)
    print("Running the command")
    with open(f"apptainer-{container_name}.log", "w") as f:
        process = subprocess.run(
            _get_command(variant="exec_command", release=release, context=context, container_name=container_name, pod=z),
            env=dict(os.environ)| ARKITEKT_APPTAINER_CONFIG,
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


def _get_command(variant: str, context, container_name=None, pod=None, release=None) -> list:
    """

    """
    # ToDo: This should only happen once. So move it outside of this function -> probably to deploy
    # Because dockers WORKDIR is not propagated to apptainer we need to get it from the docker image
    # docker inspect only works with a running docker daemon so we use skopeo (docker because we don't want to install something on the host)
    # docker_inspect_workdir = subprocess.run(["skopeo", "inspect", "--tls-verify=false", "--config", "--format='{{ .Config.WorkingDir }}'", f"docker://{flavour.image.image_string}"], text=True, capture_output=True)
    # apptainer --silent exec docker://quay.io/skopeo/stable:latest skopeo inspect --tls-verify=false --config --format='{{ .Config.WorkingDir }}' docker://alexanderschroeter/workdir-test
    docker_inspect_workdir = subprocess.run(["apptainer", "--silent", "exec", "docker://quay.io/skopeo/stable:latest", "skopeo", "inspect", "--tls-verify=false", "--config", "--format='{{ .Config.WorkingDir }}'", f"docker://{_get_flavour(release.flavours).image.image_string}"], text=True, capture_output=True)

    command = None
    if variant == "start_instance":
        command = [
            _get_apptainer_binary(),
            "instance",
            "start",
            "--writable-tmpfs",
            f"docker://{_get_flavour(release.flavours).image.image_string}",
            container_name,
        ]
        command = _get_apptainer_params(command=command, flavour=_get_flavour(release.flavours), position=3)
        print("Command: ", command)
    if variant == "stop_instance":
        command = [
            _get_apptainer_binary(),
            "instance",
            "stop",
            str(pod.pod_id),
        ]
    if variant == "exec_command":
        command = [
            _get_apptainer_binary(),
            "exec",
            "--pwd",
            str(docker_inspect_workdir.stdout.replace("'","").replace("\n","")),
            "instance://"+container_name,
            "arkitekt-next",
            "run",
            "prod",
            "--url",
            f"{context.endpoint_url}",
        ]
        command = _get_apptainer_params(command=command, flavour=_get_flavour(release.flavours), position=4)
    return command


def _get_apptainer_binary():
    binary = "/usr/local/bin/apptainer"
    if ARKITEKT_APPTAINER_CONFIG["ARKITEKT_APPTAINER_BIN"]:
        print("Using apptainer binary from config")
        binary = ARKITEKT_APPTAINER_CONFIG["ARKITEKT_APPTAINER_BIN"]

    try:
        subprocess.run([binary, "--version"], text=True, capture_output=True)
    except Exception as e:
        print("Apptainer binary not found", e)

    return binary


def _get_apptainer_params(command, flavour, position: int = -1):
    params = {
        "cuda": "--nv",
        "amd": None,
        "oneapi": None,
    }
    if params[flavour.name] != None:
        command.insert(position, params[flavour.name])
    return command


def _get_flavour(flavours):
    """
    Choose the flavour of the deployment from the list of prefered flavours choose the next best flavour.
    Currently the first available flavour of the app which is also in the list of flavours in the apptainer_config.yaml is chosen.

    ToDo:
    - Implement a way to choose the **best** flavour
        - Round robin the list of flavours set in the apptainer_config.yaml
        - Check for supported hardware and software
        - Check already used resources
    """

    def find_flavour(flavours, preferred_order):
        return next((flavour for name in preferred_order for flavour in flavours if flavour.name == name), None)

    return find_flavour(flavours, list(ARKITEKT_APPTAINER_CONFIG["ARKITEKT_FLAVOUR"].split(",")))


if __name__ == "__main__":
    pass
