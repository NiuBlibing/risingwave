// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use core::fmt;
use std::cell::RefCell;
use std::hash::{Hash, Hasher};

use risingwave_common::types::DataType;

use super::Expr;

pub type CorrelatedId = u32;

#[derive(Clone, Eq)]
/// A reference to a column outside the subquery.
///
/// `depth` is the number of of nesting levels of the subquery relative to the refered relation, and
/// should be non-zero.
///
/// `index` is the index in the refered relation.
/// `correlated_id` is the id of the related Apply operator. 0 means uninitialized.
pub struct CorrelatedInputRef {
    index: usize,
    data_type: DataType,
    depth: usize,
    correlated_id: RefCell<CorrelatedId>,
}

impl Hash for CorrelatedInputRef {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.index.hash(state);
        self.data_type.hash(state);
        self.depth.hash(state);
    }
}

impl PartialEq for CorrelatedInputRef {
    fn eq(&self, other: &Self) -> bool {
        self.index.eq(&other.index)
            && self.data_type.eq(&other.data_type)
            && self.depth.eq(&other.depth)
            && *self.correlated_id.borrow() == *other.correlated_id.borrow()
    }
}

impl CorrelatedInputRef {
    pub fn new(index: usize, data_type: DataType, depth: usize) -> Self {
        CorrelatedInputRef {
            index,
            data_type,
            depth,
            correlated_id: RefCell::new(0),
        }
    }

    /// Get a reference to the input ref's index.
    pub fn index(&self) -> usize {
        self.index
    }

    pub fn depth(&self) -> usize {
        self.depth
    }

    pub fn set_correlated_id(&self, correlated_id: CorrelatedId) {
        *self.correlated_id.borrow_mut() = correlated_id;
    }

    pub fn get_correlated_id(&self) -> CorrelatedId {
        *self.correlated_id.borrow()
    }
}

impl Expr for CorrelatedInputRef {
    fn return_type(&self) -> DataType {
        self.data_type.clone()
    }

    fn to_expr_proto(&self) -> risingwave_pb::expr::ExprNode {
        unreachable!("CorrelatedInputRef {:?} has not been decorrelated", self)
    }
}

impl fmt::Debug for CorrelatedInputRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CorrelatedInputRef")
            .field("index", &self.index)
            .field("correlated_id", &self.correlated_id.borrow())
            .finish()
    }
}
