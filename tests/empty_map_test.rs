//! An empty MapState should render as a dimmed leaf labelled "empty" (not a dead expandable node).
use flink_explorer::index::cache::{
    CachedKeyedState, CachedOperator, IndexCache, KeyPartition, StateDescriptor,
};
use flink_explorer::tui::tree::TreeView;
use ratatui::backend::TestBackend;
use ratatui::layout::Rect;
use ratatui::widgets::Block;
use ratatui::Terminal;

fn cache_with_map(values: Vec<Option<Vec<u8>>>) -> IndexCache {
    IndexCache {
        metadata_size: 0,
        metadata_mtime: 0,
        checkpoint_id: 0,
        savepoint_version: 4,
        master_states: vec![],
        operators: vec![CachedOperator {
            id: [0u8; 16],
            display_name: "op".into(),
            identifier_name: String::new(),
            parallelism: 1,
            max_parallelism: 128,
            subtask_count: 1,
            keyed_state_size: 0,
            fully_finished: false,
            has_coordinator_state: false,
            coordinator_info: None,
            states: vec![],
            keyed_state: Some(CachedKeyedState {
                descriptors: vec![StateDescriptor {
                    name: "my-map".into(),
                    state_type: "MAP".into(),
                    value_type: "Map<String,String>".into(),
                    pojo_detail: String::new(),
                    pojo_info: None,
                    entry_count: 0,
                }],
                partitions: vec![KeyPartition {
                    key: "k1".into(),
                    key_group: 0,
                    values,
                }],
                total_keys: 1,
                key_type: "String".into(),
            }),
        }],
    }
}

fn render_expanded(cache: &IndexCache) -> String {
    let mut tv = TreeView::new(cache);
    tv.expand(); // operator
    tv.move_down();
    tv.expand(); // key partition → entries appear
    let mut term = Terminal::new(TestBackend::new(120, 40)).unwrap();
    term.draw(|f| tv.render(f, Block::default(), Rect::new(0, 0, 120, 40)))
        .unwrap();
    let buf = term.backend().buffer();
    let mut s = String::new();
    for y in 0..buf.area.height {
        for x in 0..buf.area.width {
            s.push_str(buf[(x, y)].symbol());
        }
        s.push('\n');
    }
    s
}

#[test]
fn empty_map_no_data_is_leaf_labelled_empty() {
    let screen = render_expanded(&cache_with_map(vec![None]));
    let line = screen
        .lines()
        .find(|l| l.contains("my-map"))
        .expect("map line present");
    assert!(
        line.contains("empty"),
        "empty map should say 'empty': {line:?}"
    );
    assert!(
        line.contains('•') && !line.contains('⯈'),
        "empty map should be a leaf: {line:?}"
    );
}

#[test]
fn zero_count_map_blob_is_leaf_labelled_empty() {
    // A 4-byte int32(0) blob = a map with zero entries.
    let screen = render_expanded(&cache_with_map(vec![Some(vec![0, 0, 0, 0])]));
    let line = screen
        .lines()
        .find(|l| l.contains("my-map"))
        .expect("map line present");
    assert!(
        line.contains("empty"),
        "zero-count map should say 'empty': {line:?}"
    );
    assert!(
        line.contains('•') && !line.contains('⯈'),
        "zero-count map should be a leaf: {line:?}"
    );
}
