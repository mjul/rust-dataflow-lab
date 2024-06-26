extern crate timely;

use std::collections::HashSet;
use std::fmt::Display;
use std::hash::Hash;

use timely::communication::Data;
use timely::dataflow::{InputHandle, Scope};
use timely::dataflow::operators::*;
use timely::dataflow::operators::{Exchange, Input, Inspect, Probe};
use timely::dataflow::operators::aggregation::Aggregate;
use timely::dataflow::operators::capture::Extract;
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

fn capture_result_stream() -> Vec<i32> {
    let config = timely::execute::Config::process(4);
    let result = timely::execute(config, |worker| {
        let (mut input, result) = worker.dataflow(|scope| {
            let (input, stream) = scope.new_input();
            let result = stream
                .map(|x| 10 * x)
                .inspect(|x| println!("value: {:?}", x))
                .capture();
            (input, result)
        });
        let index = worker.index();

        // Only worker 0 sends data
        if index == 0 {
            for round in 0..10 {
                input.send(round);
                input.advance_to(round + 1);
            }
        }

        result
    })
        .unwrap();
    let collected: Vec<_> = result
        .join()
        .into_iter()
        .map(|x| x.unwrap().extract())
        .flat_map(|x| x.into_iter().map(|x| x.1))
        .flatten()
        .collect();
    collected
}


// Track logins and compute number of daily active users (DAU) and monthly active users (MAU).
// To keep it simple we assume an input stream with user ids and date of login, date as (year,month,day).
type UserId = u32;
type LoginDate = (u32, u32, u32);
type UserLoggedIn = (UserId, LoginDate);

fn dau_mau_network() {
    let config = timely::execute::Config::process(4);
    timely::execute(config, |worker| {
        let index = worker.index();
        let mut input_events: InputHandle<_, UserLoggedIn> = InputHandle::new();
        println!("worker {} executing...\n", index);

        worker.dataflow(|scope| {
            let by_year_month_day =
                |(uid, (year, month, day)): &UserLoggedIn| (((*year * 10000) + *month * 100 + *day) as u64);
            scope
                .input_from(&mut input_events)
                .map(|(uid, date)| (date, uid))
                .inspect(|x| println!("(K,V) stream:\t datum = {:?}", x))
                .aggregate::<_, HashSet<UserId>, _, _, _>(
                    |key, val, agg| {
                        agg.insert(val);
                    },
                    |key, agg| (key, agg),
                    |(year, month, day)| (((*year * 10000) + *month * 100 + *day) as u64),
                )
                .inspect(|x| println!("aggregated daily users:\t datum = {:?}", x))
                // TODO: count DAUs, aggregate and count MAUs
                .probe()
        });


        // Only worker 0 sends data
        if index == 0 {
            let mut epoch = 0;
            for month in 1..=3 {
                for day in 1..=5 {
                    for user_id in 1..=3 {
                        input_events.send((user_id, (2024, month, day)));
                    }
                    // advance epoch every day
                    input_events.advance_to(epoch);
                    epoch += 1;
                }
            }
        }
    }).unwrap();
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

    #[test]
    fn capture_result_stream_can_run() {
        let rs = capture_result_stream();
        assert_eq!(vec![0, 10, 20, 30, 40, 50, 60, 70, 80, 90], rs);
    }

    #[test]
    fn dau_mau_network_can_run() {
        let rs = dau_mau_network();
    }
}
