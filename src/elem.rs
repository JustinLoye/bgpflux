use bgpkit_parser::BgpElem;

#[derive(Debug, Clone, PartialEq)]
pub struct BgpStreamElem {
    pub collector_id: &'static str, // Zero-cost copy
    pub elem: BgpElem,
}

impl std::ops::Deref for BgpStreamElem {
    type Target = BgpElem;
    fn deref(&self) -> &Self::Target {
        &self.elem
    }
}
