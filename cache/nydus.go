package cache

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/errdefs"
	"github.com/moby/buildkit/session"
	"github.com/moby/buildkit/util/compression"
	digest "github.com/opencontainers/go-digest"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"

	nydusify "github.com/containerd/nydus-snapshotter/pkg/converter"
)

var nydusAnnotations = []string{nydusify.LayerAnnotationNydusBlob, nydusify.LayerAnnotationNydusBootstrap, nydusify.LayerAnnotationNydusBlobIDs}

func isNydusBlob(desc ocispecs.Descriptor) bool {
	if desc.Annotations == nil {
		return false
	}
	hasMediaType := desc.MediaType == nydusify.MediaTypeNydusBlob
	_, hasAnno := desc.Annotations[nydusify.LayerAnnotationNydusBlob]
	return hasMediaType && hasAnno
}

// compressNydus compresses a diff tar stream into nydus format (nydus blob + nydus bootstrap).
func compressNydus(ctx context.Context, comp compression.Config) (compressor, func(context.Context, content.Store) (map[string]string, error)) {
	return func(dest io.Writer, requiredMediaType string) (io.WriteCloser, error) {
			return nydusify.Convert(ctx, dest, nydusify.ConvertOption{})
		}, func(ctx context.Context, cs content.Store) (map[string]string, error) {
			annotations := map[string]string{
				// Use this annotation to identify nydus blob layer.
				nydusify.LayerAnnotationNydusBlob: "true",
			}
			return annotations, nil
		}
}

// mergeNydus does two steps:
// 1. Extracts nydus bootstrap from nydus format (nydus blob + nydus bootstrap) for each layer.
// 2. Merge all nydus bootstraps into a final bootstrap (will as an extra layer).
// The nydus bootstrap size is very small, so the merge operation is fast.
func mergeNydus(ctx context.Context, refs []*immutableRef, comp compression.Config, s session.Group) (*ocispecs.Descriptor, error) {
	if len(refs) == 0 {
		return nil, fmt.Errorf("refs can't be empty")
	}

	// Extracts nydus bootstrap from nydus format for each layer.
	var cm *cacheManager
	layers := []nydusify.Layer{}
	blobIDs := []string{}
	for _, ref := range refs {
		blobDesc, err := getBlobWithCompressionWithRetry(ctx, ref, comp, s)
		if err != nil {
			return nil, errors.Wrapf(err, "get compression blob %q", comp.Type)
		}
		ra, err := ref.cm.ContentStore.ReaderAt(ctx, blobDesc)
		if err != nil {
			return nil, errors.Wrapf(err, "get reader for compression blob %q", comp.Type)
		}
		defer ra.Close()
		if cm == nil {
			cm = ref.cm
		}
		blobIDs = append(blobIDs, blobDesc.Digest.Hex())
		layers = append(layers, nydusify.Layer{
			Digest:   blobDesc.Digest,
			ReaderAt: ra,
		})
	}

	// Merge all nydus bootstraps into a final nydus bootstrap.
	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()
		if err := nydusify.Merge(ctx, layers, pw, nydusify.MergeOption{
			WithTar: true,
		}); err != nil {
			pw.CloseWithError(errors.Wrapf(err, "merge nydus bootstrap"))
		}
	}()

	// Compress final nydus bootstrap to tar.gz and write into content store.
	chainID := refs[len(refs)-1].getChainID()
	cw, err := content.OpenWriter(ctx, cm.ContentStore, content.WithRef("nydus-merge-"+chainID.String()))
	if err != nil {
		return nil, errors.Wrap(err, "open content store writer")
	}
	defer cw.Close()

	gw := gzip.NewWriter(cw)
	uncompressedDgst := digest.SHA256.Digester()
	compressed := io.MultiWriter(gw, uncompressedDgst.Hash())
	if _, err := io.Copy(compressed, pr); err != nil {
		return nil, errors.Wrapf(err, "copy bootstrap targz into content store")
	}
	if err := gw.Close(); err != nil {
		return nil, errors.Wrap(err, "close gzip writer")
	}

	compressedDgst := cw.Digest()
	if err := cw.Commit(ctx, 0, compressedDgst, content.WithLabels(map[string]string{
		containerdUncompressed: uncompressedDgst.Digest().String(),
	})); err != nil {
		if !errdefs.IsAlreadyExists(err) {
			return nil, errors.Wrap(err, "commit to content store")
		}
	}
	if err := cw.Close(); err != nil {
		return nil, errors.Wrap(err, "close content store writer")
	}

	info, err := cm.ContentStore.Info(ctx, compressedDgst)
	if err != nil {
		return nil, errors.Wrap(err, "get info from content store")
	}

	blobIDsBytes, err := json.Marshal(blobIDs)
	if err != nil {
		return nil, errors.Wrap(err, "marshal blob ids")
	}

	desc := ocispecs.Descriptor{
		Digest:    compressedDgst,
		Size:      info.Size,
		MediaType: ocispecs.MediaTypeImageLayerGzip,
		Annotations: map[string]string{
			containerdUncompressed: uncompressedDgst.Digest().String(),
			// Use this annotation to identify nydus bootstrap layer.
			nydusify.LayerAnnotationNydusBootstrap: "true",
			// Track all blob digests for nydus snapshotter.
			nydusify.LayerAnnotationNydusBlobIDs: string(blobIDsBytes),
		},
	}

	return &desc, nil
}
