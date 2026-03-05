use bgpkit_parser::models::MetaCommunity;
use bgpkit_parser::{BgpElem, models::ElemType};
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

struct CommunitiesDisplay<'a>(&'a Option<Vec<MetaCommunity>>);

impl Display for CommunitiesDisplay<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(v) = self.0 {
            for (i, comm) in v.iter().enumerate() {
                if i > 0 {
                    f.write_str(" ")?;
                }
                write!(f, "{}", comm)?;
            }
        }
        Ok(())
    }
}

impl Display for BgpStreamElem {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // 1. Map the type to a static string
        let t = match self.elem_type {
            BgpStreamElemType::ANNOUNCE => "A",
            BgpStreamElemType::WITHDRAW => "W",
            BgpStreamElemType::RIB => "R",
        };

        // 2. Write fields. We use write_str for literals and pipe separators
        // to avoid the overhead of the write! macro's parser.
        f.write_str(t)?;
        f.write_str("|")?;

        // Use itoa for the timestamp if possible, or just write!
        write!(f, "{}", self.timestamp)?;
        f.write_str("|")?;

        write!(f, "{}", self.peer_ip)?;
        f.write_str("|")?;

        write!(f, "{}", self.peer_asn)?;
        f.write_str("|")?;

        write!(f, "{}", self.prefix)?;
        f.write_str("|")?;

        // 3. Handle Options without the wrapper overhead
        if let Some(ref v) = self.as_path {
            write!(f, "{}", v)?;
        }
        f.write_str("|")?;

        if let Some(ref v) = self.origin {
            write!(f, "{}", v)?;
        }
        f.write_str("|")?;

        if let Some(ref v) = self.next_hop {
            write!(f, "{}", v)?;
        }
        f.write_str("|")?;

        if let Some(ref v) = self.local_pref {
            write!(f, "{}", v)?;
        }
        f.write_str("|")?;

        if let Some(ref v) = self.med {
            write!(f, "{}", v)?;
        }
        f.write_str("|")?;

        // 4. Use the zero-allocation communities display
        CommunitiesDisplay(&self.communities).fmt(f)?;
        f.write_str("|")?;

        write!(f, "{}", self.atomic)?;
        f.write_str("|")?;

        if let Some(ref v) = self.aggr_asn {
            write!(f, "{}", v)?;
        }
        f.write_str("|")?;

        if let Some(ref v) = self.aggr_ip {
            write!(f, "{}", v)?;
        }
        f.write_str("|")?;

        f.write_str(self.collector_id)
    }
}
