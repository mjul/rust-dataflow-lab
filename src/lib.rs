extern crate timely;

use timely::dataflow::InputHandle;
use timely::dataflow::operators::*;
use timely::dataflow::operators::{Exchange, Input, Inspect, Probe};

// This is the simplest example from the documentation, stream numbers through a print data flow.
fn getting_started() {
    timely::example(|scope| {
        (0..10).to_stream(scope)
            .inspect(|x| println!("seen: {:?}", x));
    });
}

fn linear_steps() {
    let config = timely::execute::Config::process(4);
    timely::execute(config, |worker| {
        let index = worker.index();
        let mut input = InputHandle::new();
        let probe = worker.dataflow(|scope| {
            scope.input_from(&mut input)
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
    }).unwrap();
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

}
