<?php
namespace Raxon\Node\Trait;

use Raxon\Module\Filter as Module;

use Exception;

trait Filter {

    /**
     * @throws Exception
     */
    private function filter($record=[], $filter=[], $options=[]): mixed
    {

        $list = [];
        $list[] = $record;
        $list = Module::list($list)->where($filter);
        if(!empty($list)){
            return $record;
        }
        return false;
    }
}