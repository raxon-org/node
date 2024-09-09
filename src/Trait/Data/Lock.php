<?php

namespace Raxon\Node\Trait\Data;

use Raxon\Module\Core;
use Raxon\Module\Controller;
use Raxon\Module\Dir;
use Raxon\Module\File;

use Exception;

trait Lock {

    /**
     * @throws Exception
     */
    public function lock($class, $options=[]): bool
    {
        $options = Core::object($options, Core::OBJECT_ARRAY);
        if(!array_key_exists('lock_wait_timeout', $options)){
            $options['lock_wait_timeout'] = 60;
        }
        $name = Controller::name($class);
        $object = $this->object();
        $dir_cache = $object->config('framework.dir.temp') .
            '33' .
            $object->config('ds') .
            'Node' .
            $object->config('ds')
        ;
        $dir_lock = $dir_cache .
            'Lock' .
            $object->config('ds')
        ;
        $url_lock = $dir_lock .
            $name .
            $object->config('extension.lock')
        ;
        if(File::exist($url_lock)) {
            $timer = 0;
            $lock_wait_timeout = $options['lock_wait_timeout'];
            while(File::exist($url_lock)){
                sleep(1);
                $timer++;
                if($timer > $lock_wait_timeout){
                    throw new Exception('Lock timeout on class: ' . $name);
                }
            }
        }
        Dir::create($dir_lock, Dir::CHMOD);
        File::touch($url_lock);
        File::permission($object, [
            'dir_cache' => $dir_cache,
            'dir_lock' => $dir_lock,
            'url_lock' => $url_lock
        ]);
        $object->config('node.transaction.' . $name, true);
        return true;
    }

    /**
     * @throws Exception
     */
    public function unlock($class): bool
    {
        $name = Controller::name($class);
        $object = $this->object();
        $dir_cache = $object->config('framework.dir.temp') .
            '33' .
            $object->config('ds') .
            'Node' .
            $object->config('ds')
        ;
        $dir_lock = $dir_cache .
            'Lock' .
            $object->config('ds')
        ;
        $url_lock = $dir_lock .
            $name .
            $object->config('extension.lock')
        ;
        File::delete($url_lock);
        $object->config('node.transaction.' . $name, false);
        return true;
    }
}