import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;


public class ByteBufManager {
  private final ByteBufAllocator alloc;

  ByteBufManager(ByteBufAllocator alloc) {
    this.alloc = alloc;
  }

  public ByteBuf heapBuffer(int initialCapacity, int maxCapacity) {
    return alloc.heapBuffer(initialCapacity, maxCapacity);
  }

  public ByteBuf directBuffer(int initialCapacity, int maxCapacity) {
    return alloc.directBuffer(initialCapacity, maxCapacity);
  }

  public CompositeByteBuf compositeHeapBuffer() {
    return alloc.compositeHeapBuffer();
  }

  public CompositeByteBuf compositeDirectBuffer() {
    return alloc.compositeDirectBuffer();
  }
}
