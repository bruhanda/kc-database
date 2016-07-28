<?php

namespace KC\Database;

/**
 * Interface containing query type constants
 */
interface QueryTypeAwareInterface
{
  // Query types
  const SELECT =  1;
  const INSERT =  2;
  const UPDATE =  3;
  const DELETE =  4;
}
