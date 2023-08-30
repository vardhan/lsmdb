use std::io::Read;

pub(crate) trait ReaderExt {
    // Read a little-endian-encoded u32
    fn read_u32_le(&mut self) -> Result<u32, std::io::Error>;
    fn read_u8(&mut self) -> Result<u8, std::io::Error>;
    // Allocates a new vector of size `length` and reads into it.
    fn read_u8s(&mut self, length: usize) -> Result<Vec<u8>, std::io::Error>;
}
impl<T: Read> ReaderExt for T {
    fn read_u32_le(&mut self) -> Result<u32, std::io::Error> {
        let mut encoded_num: [u8; 4] = Default::default();
        self.read_exact(&mut encoded_num)?;
        Ok(u32::from_le_bytes(encoded_num))
    }

    fn read_u8(&mut self) -> Result<u8, std::io::Error> {
        let mut encoded_num: [u8; 1] = Default::default();
        self.read_exact(&mut encoded_num)?;
        Ok(encoded_num[0])
    }

    fn read_u8s(&mut self, length: usize) -> Result<Vec<u8>, std::io::Error> {
        let mut bytes = Vec::<u8>::new();
        bytes.resize(length, 0);
        self.read(bytes.as_mut_slice())?;
        Ok(bytes)
    }
}
