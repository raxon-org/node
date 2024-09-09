<?php

namespace Raxon\Org\Node\Trait;

use Raxon\Org\Module\Controller;
use Raxon\Org\Module\File;

use Exception;

use Raxon\Org\Exception\ObjectException;

trait Expand {

    /**
     * @throws ObjectException
     * @throws Exception
     */
    public function expand($class, $role, $options = []): array | bool
    {
        $object = $this->object();
        $name = Controller::name($class);
        $url = $object->config('project.dir.node') .
            'Data' .
            $object->config('ds') .
            $name .
            $object->config('extension.json')
        ;
        $data = $object->data_read($url);
        if($data){
            $count = count($data->data($name));
            $byte = $data->write($url);
            return [
                'count' => $count,
                'byte' => $byte,
                'size' => File::size_format($byte)
            ];
        }
        return false;
    }
}