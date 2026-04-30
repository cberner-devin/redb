use crate::tree_store::PageNumber;
use crate::tree_store::btree_base::Checksum;

pub(super) struct RetainNode {
    pub(super) page: PageNumber,
    pub(super) checksum: Checksum,
    // Upper bound for this subtree. Parent branch separators are rebuilt from
    // it. Only the final subtree in an ordered list may use None; separator
    // keys on older branch pages can be stale upper bounds, so retain must not
    // rely on this being the exact maximum key unless the node was just rebuilt.
    pub(super) upper_key: Option<Vec<u8>>,
}

#[derive(Copy, Clone, Debug)]
pub(super) enum RetainMergeSide {
    Left,
    Right,
}

#[derive(Copy, Clone, Debug)]
pub(super) struct RetainPartial {
    // An underfilled rebuilt node only retries adjacent sides that have not
    // already been repacked with this node. This prevents normalization from
    // cycling on variable-sized entries that cannot be balanced further.
    pub(super) try_left: bool,
    pub(super) try_right: bool,
}

#[derive(Copy, Clone)]
pub(super) struct RetainMergeContext {
    pub(super) left_partial: Option<RetainPartial>,
    pub(super) right_partial: Option<RetainPartial>,
}

impl RetainPartial {
    pub(super) fn new() -> Self {
        Self {
            try_left: true,
            try_right: true,
        }
    }

    pub(super) fn after_trying(mut self, side: RetainMergeSide) -> Self {
        match side {
            RetainMergeSide::Left => self.try_left = false,
            RetainMergeSide::Right => self.try_right = false,
        }
        self
    }

    pub(super) fn left_only(mut self) -> Option<Self> {
        self.try_right = false;
        self.try_left.then_some(self)
    }

    pub(super) fn right_only(mut self) -> Option<Self> {
        self.try_left = false;
        self.try_right.then_some(self)
    }
}

impl RetainMergeContext {
    pub(super) fn new(left: &RetainSubtree, right: &RetainSubtree) -> Self {
        Self {
            left_partial: left.partial,
            right_partial: right.partial,
        }
    }
}

// A sealed B-tree subtree, annotated with its distance from the original retain
// walk root. This lets retain stitch unchanged boundary subtrees without
// measuring their distance to leaves.
pub(super) struct RetainSubtree {
    pub(super) node: RetainNode,
    pub(super) depth: u32,
    // Some if this page is below the merge threshold and still has an adjacent
    // side worth trying.
    pub(super) partial: Option<RetainPartial>,
}

pub(super) struct RetainSubtreeBuilder {
    // In-progress left-to-right replacement stream. The walker only appends
    // retained entries and sealed unchanged subtrees; the builder owns turning
    // those pieces into pages and normalizing adjacent subtrees.
    pub(super) subtrees: Vec<RetainSubtree>,
    pub(super) leaf_entries: Vec<(Vec<u8>, Vec<u8>)>,
    pub(super) leaf_depth: Option<u32>,
}

impl RetainSubtreeBuilder {
    pub(super) fn new() -> Self {
        Self {
            subtrees: vec![],
            leaf_entries: vec![],
            leaf_depth: None,
        }
    }

    pub(super) fn from_subtrees(subtrees: Vec<RetainSubtree>) -> Self {
        Self {
            subtrees,
            leaf_entries: vec![],
            leaf_depth: None,
        }
    }

    pub(super) fn len(&self) -> usize {
        self.subtrees.len()
    }

    pub(super) fn push(&mut self, subtree: RetainSubtree) {
        self.subtrees.push(subtree);
    }

    pub(super) fn debug_state(&self) -> Vec<(u32, Option<RetainPartial>)> {
        debug_assert!(self.leaf_entries.is_empty());
        self.subtrees
            .iter()
            .map(|subtree| (subtree.depth, subtree.partial))
            .collect()
    }

    pub(super) fn first_pair_needing_merge(&mut self) -> Option<usize> {
        loop {
            if self.subtrees.len() <= 1 {
                return None;
            }

            if let Some(index) = self
                .subtrees
                .windows(2)
                .position(|pair| pair[0].depth != pair[1].depth)
            {
                return Some(index);
            }

            let index = self
                .subtrees
                .iter()
                .position(|subtree| subtree.partial.is_some())?;

            let partial = self.subtrees[index].partial.unwrap();
            if partial.try_right && index + 1 < self.subtrees.len() {
                return Some(index);
            }
            if partial.try_left && index > 0 {
                return Some(index - 1);
            }
            self.subtrees[index].partial = None;
        }
    }

    pub(super) fn remove_pair(&mut self, index: usize) -> (RetainSubtree, RetainSubtree) {
        let left = self.subtrees.remove(index);
        let right = self.subtrees.remove(index);
        (left, right)
    }

    pub(super) fn splice(&mut self, index: usize, replacement: Vec<RetainSubtree>) {
        self.subtrees.splice(index..index, replacement);
    }

    pub(super) fn into_vec(self) -> Vec<RetainSubtree> {
        debug_assert!(self.leaf_entries.is_empty());
        self.subtrees
    }
}
