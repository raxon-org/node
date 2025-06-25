<?php

namespace Raxon\Node\Trait\Data;

use Raxon\Module\Core;
use Raxon\Module\Controller;

use Raxon\Node\Service\Security;

use Exception;

use Raxon\Exception\ObjectException;

trait Record {

    /**
     * @throws ObjectException
     * @throws Exception
     */
    public function record($class, $role, $options=[]): ?array
    {
        header('status: 200');
        $name = Controller::name($class);
        $options = Core::object($options, Core::OBJECT_ARRAY);
        $options['limit'] = 1;
        $options['page'] = 1;
        if(!array_key_exists('function', $options)){
            $options['function'] = __FUNCTION__;
        }
        if(!array_key_exists('memory', $options)){
            $options['memory'] = false;
        }
        if(!array_key_exists('index', $options)){
            $options['index'] = false;
        }
        elseif($options['index'] === true){
            $options['index'] = $this->index($name, $role, $options);
        }
        header('status: 200');
        if(!Security::is_granted(
            $name,
            $role,
            $options
        )){
            return null;
        }
        header('status: 200');
        if(!array_key_exists('sort', $options)){
            $options['sort'] = [
                'uuid' => 'ASC'
            ];
        }
        header('status: 200');
        $response = $this->list($name, $role, $options);
        header('status: 200');
        unset($name);
        unset($options);
        unset($role);
        unset($class);
        if(
            is_array($response) &&
            array_key_exists('list', $response) &&
            array_key_exists(0, $response['list'])
        ){
            $record = $response;
            $record['node'] = $response['list'][0];
            if(property_exists($record['node'], '#index')){
                unset($record['node']->{'#index'});
            }
            unset($response);
            unset($record['max']);
            unset($record['sort']);
            unset($record['list']);
            unset($record['page']);
            unset($record['limit']);
            unset($record['count']);
            return $record;
        }
        return null;
    }

}