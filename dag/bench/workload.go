package bench

import (
	"encoding/json"
	"fmt"
)

// Action is one incoming write: an identity and the record it publishes, with
// the refusal the evaluation must reach.
type Action struct {
	Name     string
	Identity string
	Raw      []byte
	Want     Refusal
}

// The records. Each is a shape from the registry, named in the write-up by
// size: the small node is a tag (208 bytes, 9 fields), the edge tags an asset
// (310 bytes, 10 fields) and the large node is a video clip's asset record
// (726 bytes, 21 fields). TestRecordSizes holds them to those sizes.
func tagRecord(id string) map[string]any {
	return map[string]any{
		"node_id": id, "schema_id": "sch_Tag_v1", "tenant_id": "acme",
		"created_at": "2026-10-19T09:00:00Z",
		"payload":    map[string]any{"namespace": "exercise", "value": "weighted hip thrust", "origin": "vlm", "confidence": 0.91},
	}
}

func edgeRecord(id string) map[string]any {
	return map[string]any{
		"edge_id": id, "schema_id": "sch_Edge_Derivation_v1", "tenant_id": "acme",
		"source_id": "nd_assetAAAAAAAAAAAAAAAA", "target_id": "nd_tagBBBBBBBBBBBBBBBBBB",
		"created_at": "2026-10-19T09:00:00Z",
		"payload":    map[string]any{"relation": "tagged", "namespace": "exercise", "value": "weighted hip thrust", "served_by": "gpu-rig"},
	}
}

func assetRecord(id string) map[string]any {
	return map[string]any{
		"node_id": id, "schema_id": "sch_Asset_ProRes_v2", "tenant_id": "acme",
		"created_at":   "2026-10-19T09:00:00Z",
		"content_hash": "sha256:9f2c1e4b7a0d3c6e8f1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f70",
		"payload": map[string]any{
			"codec": "HEVC", "origin": "studio_a_handheld_camera_1",
			"storage_uri":        "s3://example-vault/ingest/DJI_20261012093015_0042_D.MP4",
			"analysis_proxy_uri": "s3://example-vault/proxy/DJI_20261012093015_0042_D.mp4",
			"duration_s":         37.2, "width": 3840, "height": 2160, "fps": 59.94,
			"nb_frames": 2230, "fps_num": 60000, "fps_den": 1001,
			"basename":        "DJI_20261012093015_0042_D.MP4",
			"hash_xxh3":       "0123456789abcdef0123456789abcdef",
			"hash_xxh3_scope": "first_16MiB+last_16MiB+size",
			"ingest_state":    "indexed",
			"dead_air":        [][]float64{{0, 1.2}, {35.8, 37.2}},
		},
	}
}

// unknownRecord has a schema_id no case of Kind knows: its family is
// "unknown", and no stream binds <root>.unknown.>.
func unknownRecord(id string) map[string]any {
	return map[string]any{
		"node_id": id, "schema_id": "sch_Widget_v1", "tenant_id": "acme",
		"payload": map[string]any{"name": "a kind nobody registered"},
	}
}

// badTagRecord fails its schema: the payload has no value.
func badTagRecord(id string) map[string]any {
	r := tagRecord(id)
	r["payload"] = map[string]any{"namespace": "exercise"}
	return r
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// Record builds one record of the named shape with the given id.
func Record(shape, id string) []byte {
	switch shape {
	case "tag":
		return mustJSON(tagRecord(id))
	case "edge":
		return mustJSON(edgeRecord(id))
	case "asset":
		return mustJSON(assetRecord(id))
	case "unknown":
		return mustJSON(unknownRecord(id))
	case "bad_tag":
		return mustJSON(badTagRecord(id))
	}
	panic(fmt.Sprintf("no shape %q", shape))
}

// Mix is the benchmark's workload: twenty actions, sixteen allowed and four
// refused, one of each refusal the evaluation can reach plus a second grant
// refusal. Each worker cycles through it.
func Mix() []Action {
	a := func(name, identity, shape, id string, want Refusal) Action {
		return Action{Name: name, Identity: identity, Raw: Record(shape, id), Want: want}
	}
	var out []Action
	for i := 0; i < 6; i++ {
		out = append(out, a("the authority writes a tag", "authority", "tag", fmt.Sprintf("nd_tag%016d", i), Allowed))
	}
	for i := 0; i < 5; i++ {
		out = append(out, a("the authority writes an edge", "authority", "edge", fmt.Sprintf("ed_edge%016d", i), Allowed))
	}
	for i := 0; i < 3; i++ {
		out = append(out, a("the GPU worker writes an asset", "gpu-worker", "asset", fmt.Sprintf("nd_asset%016d", i), Allowed))
	}
	out = append(out,
		a("an editing workstation writes an asset", "workstation-render-1", "asset", "nd_assetws00000000001", Allowed),
		a("the authority writes an asset", "authority", "asset", "nd_assetauth000000001", Allowed),
		a("the GPU worker writes a tag", "gpu-worker", "tag", "nd_taggpuworker000001", NotGranted),
		a("a browser worker writes an asset", "browser", "asset", "nd_assetbrowser000001", NotGranted),
		a("the authority writes an unregistered kind", "authority", "unknown", "nd_widget000000000001", NoStream),
		a("the authority writes a tag that fails its schema", "authority", "bad_tag", "nd_tagbad000000000001", SchemaRejected),
	)
	return out
}
