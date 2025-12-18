package utils

import java.util.BitSet
import scala.util.hashing.MurmurHash3

object BloomFilter {

  val n: Int = 500000
  val k: Int = 7

  private val bits = new BitSet(n)

  private def hash(value: String, seed: Int): Int = {
    Math.abs(MurmurHash3.stringHash(value, seed) % n)
  }

  def mightContain(value: String): Boolean = {
    (0 until k).forall { i =>
      bits.get(hash(value, i))
    }
  }

  def add(value: String): Unit = {
    (0 until k).foreach { i =>
      bits.set(hash(value, i))
    }
  }
}
