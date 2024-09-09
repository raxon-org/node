<?php

namespace Event\Raxon\Node;

use Event\Raxon\Framework\Email;

use Raxon\App;
use Raxon\Config;

use Exception;

use Raxon\Exception\ObjectException;

class Create {

    /**
     * @throws ObjectException
     * @throws Exception
     */
    public static function error(App $object, $event, $options=[]): void
    {
        if($object->config(Config::POSIX_ID) !== 0){
            return;
        }
        Create::notification($object, $event, $options);
    }

    /**
     * @throws Exception
     */
    public static function notification(App $object, $event, $options=[]): void
    {
        $action = $event->get('action');
        Email::queue(
            $object,
            $action,
            $options
        );
    }
}