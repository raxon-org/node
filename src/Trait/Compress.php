<?php

namespace Raxon\Org\Node\Trait;

use Raxon\Org\Config;
use Raxon\Org\Module\Controller;
use Raxon\Org\Module\File;

use Exception;

use Raxon\Org\Exception\ObjectException;

trait Compress {

    /**
     * @throws ObjectException
     * @throws Exception
     */
    public function compress($class, $role, $options = []): array | bool
    {
        $object = $this->object();
        $name = Controller::name($class);
        $url = $object->config('project.dir.node') .
            'Data' .
            $object->config('ds') .
            $name .
            $object->config('extension.json');

        $data = $object->data_read($url);
        if($data){
            $url .= '.gz';
            $count = count($data->data($name));
            $write = $data->write($url, [
                'compact' => true,
                'compress' => true
            ]);
            File::permission($object, [
                'url' => $url
            ]);
            $duration = microtime(true) - $object->config('time.start');
            return [
                'count' => $count,
                'byte' => $write['byte'],
                'compression' => round($write['original'] / $write['byte'], 2) . ' x',
                'size' => File::size_format($write['byte']),
                'duration' => round($duration, 2) . ' sec',
                'speed' => File::size_format($write['byte'] / $duration) . ' per sec'
            ];
        }
        return false;
    }
}