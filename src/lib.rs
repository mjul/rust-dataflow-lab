extern crate timely;

use std::fmt::Display;
use std::hash::Hash;

use timely::communication::Data;
use timely::dataflow::operators::capture::Extract;
use timely::dataflow::operators::*;
use timely::dataflow::operators::{Exchange, Input, Inspect, Probe};
use timely::dataflow::{InputHandle, Scope};
use timely::progress::Timestamp;
use timely::ExchangeData;

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

// This uses the `accumulate` operator to sum up the measurements for each epoch (month)
fn accumulate_by_epoch() {
    let config = timely::execute::Config::process(4);
    timely::execute(config, |worker| {
        let index = worker.index();
        let mut input: timely::dataflow::InputHandle<_, Measurement> = InputHandle::new();
        worker.dataflow(|scope| {
            let by_year_month =
                |(year, month, value): &Measurement| (((*year * 100) + *month) as u64);
            scope
                .input_from(&mut input)
                .exchange(by_year_month)
                .inspect(|x| println!("exchanged:\t datum = {:?}", x))
                .accumulate((0u32, 0u32, 0u64), |sum, data| {
                    for &(year, month, val) in data.iter() {
                        // Note: this only works because all data for a month is in the same epoch
                        sum.0 = year;
                        sum.1 = month;
                        sum.2 += val;
                    }
                })
                .inspect(move |x| println!("worker {}:\t sum = {:?}", index, x))
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
                    // advance epoch every month
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
    }

    #[test]
    fn linear_steps_can_run() {
        linear_steps();
    }

    #[test]
    fn accumulate_by_epoch_can_run() {
        accumulate_by_epoch();
    }
}
