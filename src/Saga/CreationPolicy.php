<?php

namespace Sunspikes\Broadway\Saga;

class CreationPolicy
{
    /** Always create a new Saga, even if one already exists. */
    const ALWAYS = 'always';

    /** Only create a new Saga instance if none can be found. */
    const IF_NONE_FOUND = 'if_none_found';

    /** Never create a new Saga instance, even if none exists. */
    const NONE = 'none';
}
