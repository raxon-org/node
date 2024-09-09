<?php

namespace Raxon\Node\Trait\Data;

use Raxon\Module\Controller;
use Raxon\Module\Core;

use Exception;

trait Count {

    /**
     * @throws Exception
     */
    public function count($class, $role, $options=[]): int
    {
        $options = Core::object($options, Core::OBJECT_ARRAY);
        $count = 0;
        if(!array_key_exists('function', $options)){
            $options['function'] = 'count';
        }
        if(!array_key_exists('limit', $options)){
            $options['limit'] = '*';
        }
        unset($options['page']);
        $response = $this->list($class, $role, $options);
        if(
            !empty($response) &&
            is_array($response) &&
            array_key_exists('count', $response)
        ){
            $count = $response['count'];
        }
        return $count;
    }
}