# FUSE (File System in User Space)
Project Title：Multi-client file system based on FUSE
This is the course project from "Principle of Computer System Design"
instructor: Prof. Renato Figuriredo
Description
In FUSE, the correct concurrent operations is of great importance when multiple clients are introduced. Without proper mechanism, concurrent accesses problem can produce unpredictable results, which may lead to the failure of FUSE. In this paper, we import POSIX standard file-locking mechanism to guarantee the atomic file operations on objects stored in the server. In addition, we extend our work to multi-server using hash mapping. Finally, we performed the robust testings to illustrate the accuracy of our design.
