// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use log::warn;
use once_cell::sync::OnceCell;
use prometheus::core::{
    AtomicI64, Collector, GenericCounter, GenericCounterVec, GenericGauge, GenericGaugeVec,
};
use prometheus::{Encoder, Opts, Registry, TextEncoder};

use crate::{err_box, CommonResult};

pub type Counter = GenericCounter<AtomicI64>;
pub type CounterVec = GenericCounterVec<AtomicI64>;
pub type Gauge = GenericGauge<AtomicI64>;
pub type GaugeVec = GenericGaugeVec<AtomicI64>;

static REGISTRY: OnceCell<Registry> = OnceCell::new();

pub struct Metrics;

impl Metrics {
    pub fn init() {
        REGISTRY.get_or_init(Registry::new);
    }

    fn register(c: Box<dyn Collector>) -> CommonResult<()> {
        match REGISTRY.get() {
            Some(v) => {
                match v.register(c) {
                    Ok(_) => (),
                    Err(e) => warn!("register {}", e),
                }
                Ok(())
            }

            None => err_box!("Prometheus registry not init"),
        }
    }

    pub fn new_counter<T: Into<String>>(name: T, help: T) -> CommonResult<Counter> {
        let c = Counter::new(name, help)?;
        Self::register(Box::new(c.clone()))?;
        Ok(c)
    }

    pub fn new_counter_vec<T: Into<String>>(
        name: T,
        help: T,
        label_names: &[&str],
    ) -> CommonResult<CounterVec> {
        let c = CounterVec::new(Opts::new(name, help), label_names)?;
        Self::register(Box::new(c.clone()))?;
        Ok(c)
    }

    pub fn new_gauge<T: Into<String>>(name: T, help: T) -> CommonResult<Gauge> {
        let g = Gauge::new(name, help)?;
        Self::register(Box::new(g.clone()))?;
        Ok(g)
    }

    pub fn new_gauge_vec<T: Into<String>>(
        name: T,
        help: T,
        label_names: &[&str],
    ) -> CommonResult<GaugeVec> {
        let g = GaugeVec::new(Opts::new(name, help), label_names)?;
        Self::register(Box::new(g.clone()))?;
        Ok(g)
    }

    pub fn text_output() -> CommonResult<String> {
        if let Some(r) = REGISTRY.get() {
            let mut buffer = Vec::new();
            let encoder = TextEncoder::new();
            let metric_families = r.gather();
            encoder.encode(&metric_families, &mut buffer)?;

            let output = String::from_utf8(buffer.clone())?;
            Ok(output)
        } else {
            err_box!("Registry not init")
        }
    }
}

#[cfg(test)]
mod test {
    use crate::common::Metrics;

    #[test]
    fn sample() {
        Metrics::init();

        let counter = Metrics::new_counter("m1", "m1").unwrap();
        let counter_vec = Metrics::new_counter_vec("m2", "m2", &["l1"]).unwrap();
        let gauge = Metrics::new_gauge("g1", "g1").unwrap();
        let gauge_vec = Metrics::new_gauge_vec("g2", "g2", &["l2"]).unwrap();

        counter.inc_by(100);
        counter_vec.with_label_values(&["v1"]).inc();
        gauge.set(1000);
        gauge_vec.with_label_values(&["v2"]).set(2000);

        assert_eq!(counter.get(), 100);
        assert_eq!(gauge.get(), 1000);

        let output = Metrics::text_output().unwrap();
        println!("output = {}", output)
    }
}
