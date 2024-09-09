<?php

namespace Event\Raxon\Org\Node;

use Event\Raxon\Org\Framework\Email;

use Raxon\Org\App;
use Raxon\Org\Config;

use Exception;

use Raxon\Org\Exception\ObjectException;

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