extern crate timely;

use std::collections::HashMap;
use std::fmt::Display;
use std::hash::Hash;

use timely::communication::Data;
use timely::dataflow::{InputHandle, Scope};
use timely::dataflow::operators::*;
use timely::dataflow::operators::{Exchange, Input, Inspect, Probe};
use timely::ExchangeData;
use timely::progress::Timestamp;

// This is the simplest example from the documentation, stream numbers through a print data flow.
fn getting_started() {
    timely::example(|scope| {
        (0..10)
            .to_stream(scope)
            .inspect(|x| println!("seen: {:?}", x));
    });
}

fn linear_steps() {
    let config = timely::execute::Config::process(4);
    timely::execute(config, |worker| {
        let index = worker.index();
        let mut input = InputHandle::new();
        let probe = worker.dataflow(|scope| {
            scope
                .input_from(&mut input)
                // exchange has to be used to make sure that the data is shuffled between workers
                .exchange(|x| *x)
                .map(|x| x * x)
                .map(|x| x + 1)
                .inspect(move |x| println!("worker {}:\t $x^2+1 = {}$", index, x))
                .probe()
        });
        for round in 0..10 {
            // Only worker 0 sends data
            if index == 0 {
                input.send(round);
            }
            input.advance_to(round + 1);
        }
    })
        .unwrap();
}

// Stream some measurements through the dataflow, collect sums by month
// Keep it simple to focus on dataflow, not custom types or time/calendar libs
// (we will get to custom types later)
// Here, we use a tuple (year, month, value) to represent a measurement
type Measurement = (u32, u32, u64);


/*
                        let count_entry = sums_by_year_by_month
                            .entry(year.clone())
                            .or_insert(HashMap::new())
                            .entry(month.clone())
                            .or_insert(0u64);
                        *count_entry += value;

 */

fn split_by_month() {
    let config = timely::execute::Config::process(4);
    timely::execute(config, |worker| {
        let index = worker.index();
        let mut input: timely::dataflow::InputHandle<_, Measurement> = InputHandle::new();
        let probe = worker.dataflow(|scope| {
            let mut sums_by_year_by_month: HashMap<u32, HashMap<u32, u64>> = HashMap::new();
            let by_year_month = |(year, month, value): &Measurement| (((*year * 100) + *month) as u64);
            scope
                .input_from(&mut input)
                .exchange(by_year_month)
                .inspect(move |(year, month, value): &(u32, u32, u64)| {
                    let sum = sums_by_year_by_month
                        .get(&year).unwrap_or(&HashMap::new())
                        .get(&month).unwrap_or(&0)
                        .clone();
                    println!("worker {}:\t {} {} \t obs {} -> {}", index, year, month, value, sum);
                })
                .probe()
        });

        // Only worker 0 sends data
        // Send some random measurements
        if index == 0 {
            let mut epoch = 0;
            for year in 2022..=2023 {
                for month in 1..=12 {
                    for i in 0..3 {
                        input.send((year, month, (2 * month + i) as u64));
                    }
                    input.advance_to(epoch);
                    epoch += 1;
                }
            }
        }
    })
        .unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn getting_started_can_run() {
        getting_started();
        assert_eq!(true, true);
    }

    #[test]
    fn linear_steps_can_run() {
        linear_steps();
        assert_eq!(true, true);
    }

    #[test]
    fn split_by_month_can_run() {
        split_by_month();
        assert_eq!(true, true);
    }
}
