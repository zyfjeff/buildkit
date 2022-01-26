## Nydus image formats

Nydus is an OCI/Docker-compatible accelerated image format provided by the Dragonfly [image-service](https://github.com/dragonflyoss/image-service) project, which offers the ability to pull image data on-demand, without waiting for the entire image pull to complete and then start the container. It has been put in production usage and shown vast improvements to significantly reduce the overhead costs on time, network, disk IO of pulling image or starting container.

Nydus image can be flexibly configured as a FUSE-based user-space filesystem or in-kernel [EROFS](https://www.kernel.org/doc/html/latest/filesystems/erofs.html) (from Linux kernel v5.16) with Nydus daemon in user-space, integrating with VM-based container runtime like [KataContainers](https://katacontainers.io/) is much easier.

## Creating Nydus images

### Building Nydus with BuildKit

On buildkitd side, download `nydus-image` binary from [nydus release page](https://github.com/dragonflyoss/image-service/releases) (require v2.0.0 or higher), then put the `nydus-image` binary path into $PATH or specifying it on `NYDUS_BUILDER` environment variable for buildkitd:

```
env NYDUS_BUILDER=/path/to/nydus-image buildkitd ...
```

On buildctl side, export Nydus image as the one of compression types by specifying `compression=nydus` option:

```
buildctl build ... \
  --output type=image,name=docker.io/username/image,push=true,compression=nydus,oci-mediatypes=true
```

### Known limitations

- The export of Nydus image and runtime (e.g. [docker](https://github.com/dragonflyoss/image-service/tree/master/contrib/docker-nydus-graphdriver), [containerd](https://github.com/containerd/nydus-snapshotter), etc.) is currently only supported on linux platform.
- Nydus image layers cannot be mixed with other compression types in the same image, so the `force-compression=true` option is automatically enabled when exporting both Nydus compression type and other compression types.
- Specifying a Nydus image as a base image in a Dockerfile is not currently supported (i.e. lazily pull using a Nydus image).

### Other ways to create Nydus images

Pre-converted Nydus images are available at [`ghcr.io/dragonflyoss/image-service` repository](https://github.com/orgs/dragonflyoss/packages?ecosystem=container) (mainly for testing purpose).

[`Nydusify`](https://github.com/dragonflyoss/image-service/blob/master/docs/nydusify.md) The Nydusify CLI tool pulls & converts an OCIv1 image into a Nydus image, and pushes Nydus image to registry.

[`Harbor Acceld`](https://github.com/goharbor/acceleration-service) Harbor acceld provides a general service to convert OCIv1 image to acceleration image like [Nydus](https://github.com/dragonflyoss/image-service) and [eStargz](https://github.com/containerd/stargz-snapshotter) etc.
