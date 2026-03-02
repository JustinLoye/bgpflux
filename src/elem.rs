use bgpkit_parser::{BgpElem, models::ElemType, models::elem::option_to_string_communities};
use std::fmt::{Display, Formatter};

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

// This struct is copied from bgpkit-parser
// Original source: https://github.com/bgpkit/bgpkit-parser/
// Copyright (c) 2021 Mingwei Zhang
// Licensed under the MIT License
/// `OptionToStr` is a helper struct that wraps an `Option` and provides a convenient
/// way to convert its value to a string representation.
///
/// # Generic Parameters
///
/// - `'a`: The lifetime parameter that represents the lifetime of the wrapped `Option` value.
///
/// # Fields
///
/// - `0: &'a Option<T>`: The reference to the wrapped `Option` value.
struct OptionToStr<'a, T>(&'a Option<T>);

impl<T: Display> Display for OptionToStr<'_, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            None => Ok(()),
            Some(x) => write!(f, "{x}"),
        }
    }
}

// This trait is adapted from bgpkit-parser
// Original source: https://github.com/bgpkit/bgpkit-parser/
// Copyright (c) 2021 Mingwei Zhang
// Licensed under the MIT License
impl Display for BgpStreamElem {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let t = match self.elem_type {
            BgpStreamElemType::ANNOUNCE => "A",
            BgpStreamElemType::WITHDRAW => "W",
            BgpStreamElemType::RIB => "R", // Addition
        };
        write!(
            f,
            "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
            t,
            &self.timestamp,
            &self.peer_ip,
            &self.peer_asn,
            &self.prefix,
            OptionToStr(&self.as_path),
            OptionToStr(&self.origin),
            OptionToStr(&self.next_hop),
            OptionToStr(&self.local_pref),
            OptionToStr(&self.med),
            option_to_string_communities(&self.communities),
            self.atomic,
            OptionToStr(&self.aggr_asn),
            OptionToStr(&self.aggr_ip),
            &self.collector_id // Addition
        )
    }
}
