use std::io::{self, prelude::*, BufReader, Write};
use std::net::TcpStream;

fn main() {
    // connect
    // Struct used to start requests to the server.
    // Check TcpStream Connection to the server
    let mut stream = TcpStream::connect("127.0.0.1:7878").unwrap();

    loop {
        let mut cmd = String::new();

        let _size = io::stdout().write("> ".as_bytes()).unwrap();
        io::stdout().flush().unwrap();

        io::stdin()
            .read_line(&mut cmd)
            .expect("failed to read command");

        // Write the message so that the receiver can access it.
        let _size = stream
            .write(cmd.as_bytes())
            .expect("failed to write command");

        // Add Buffering so that the receiver can read the message from the stream.
        let mut reader = BufReader::new(&stream);
        let mut buf: Vec<u8> = Vec::new();

        if reader.read_until(b'\n', &mut buf).unwrap() == 0 {
            break;
        }

        let buf = String::from_utf8_lossy(&buf);
        let buf = buf.replace("\\n", "\n");

        println!("{}", buf.strip_suffix("\n").unwrap());
    }
}
