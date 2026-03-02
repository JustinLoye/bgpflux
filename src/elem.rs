use bgpkit_parser::{BgpElem, models::ElemType};

// This struct is adapted from bgpkit-parser
// Original source: https://github.com/bgpkit/bgpkit-parser/
// Copyright (c) 2021 Mingwei Zhang
// Licensed under the MIT License
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename = "lowercase"))]
pub enum BgpStreamElemType {
    ANNOUNCE,
    WITHDRAW,
    RIB, // This part has changed
}

impl From<ElemType> for BgpStreamElemType {
    fn from(value: ElemType) -> Self {
        match value {
            ElemType::ANNOUNCE => BgpStreamElemType::ANNOUNCE,
            ElemType::WITHDRAW => BgpStreamElemType::WITHDRAW,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BgpStreamElem {
    pub collector_id: &'static str,   // Zero-cost copy
    pub elem_type: BgpStreamElemType, // Shadows BgpElem.elem_type
    pub elem: BgpElem,
}

impl std::ops::Deref for BgpStreamElem {
    type Target = BgpElem;
    fn deref(&self) -> &Self::Target {
        &self.elem
    }
}
