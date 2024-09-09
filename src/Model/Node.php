<?php
namespace Raxon\Node\Model;

use Raxon\App;

use Raxon\Module\Data as Storage;
use Raxon\Module\Template\Main;

use Raxon\Node\Trait\Data;
use Raxon\Node\Trait\Role;

class Node extends Main {
    use Data;
    use Role;

    public function __construct(App $object){
        $this->object($object);
        $this->storage(new Storage());
    }
}