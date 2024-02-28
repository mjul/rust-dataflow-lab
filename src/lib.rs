
extern crate timely;

use timely::dataflow::operators::*;

// This is the simplest example from the documentation, stream numbers through a print data flow.
fn getting_started() {
    timely::example(|scope| {
        (0..10).to_stream(scope)
            .inspect(|x| println!("seen: {:?}", x));
    });
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn getting_started_can_run() {
        getting_started();
        assert_eq!(true, true);
    }

}
